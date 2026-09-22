/*
 * The beacon's own behaviour, run under node --test.
 *
 * `sdk_test.go` only ever checked that the file is SERVED — content type, ETag, CORS. What
 * the script DOES had no test at all, and the thing it decides is what a page view is: the
 * one number every other number is read against. Two bugs lived in six lines of it for
 * months (a filter change counted as a view; an A/B arm at the same address did not), so it
 * gets a harness.
 *
 * A vm sandbox rather than a headless browser: the beacon touches a handful of globals and
 * stubbing them takes less code than driving Chrome, runs in milliseconds, and lets a test
 * decide exactly when a batch flushes.
 */
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { createContext, runInContext } from 'node:vm';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const SOURCE = readFileSync(join(dirname(fileURLToPath(import.meta.url)), 'analytics.js'), 'utf8');

/**
 * A page with the beacon on it.
 *
 * `setTimeout` is fake on purpose. The beacon lingers two seconds before sending, and half
 * of what is being tested here is what happens to an event while it is still in the queue —
 * so a test has to be able to hold the flush open and then let it go, rather than sleep.
 */
function load({ path = '/', search = '', attrs = {}, framed = false, docHeight = 4626 } = {}) {
	const sent = [];
	const timers = [];
	const listeners = {};

	const location = {
		href: 'https://site.example' + path + search,
		pathname: path,
		search,
		hostname: 'site.example',
	};

	const attributes = {
		'data-pageviews': 'true',
		'data-autocapture': 'false',
		'data-pageleaves': 'false',
		'data-heatmaps': 'false',
		...attrs,
	};

	const script = {
		src: 'https://analytics.example/a.js',
		getAttribute: name => (name in attributes ? attributes[name] : null),
	};

	const posted = [];
	const sandbox = {
		document: {
			currentScript: script,
			referrer: '',
			addEventListener: () => {},
			documentElement: { clientWidth: 1440, scrollHeight: docHeight },
			body: { scrollHeight: docHeight },
			getElementsByTagName: () => [script],
		},
		location,
		navigator: {},
		history: {},
		sessionStorage: {
			getItem: () => null,
			setItem: () => {},
		},
		crypto: { getRandomValues: b => b.fill(7) },
		fetch: (_url, opts) => {
			sent.push(JSON.parse(opts.body));
			return { catch: () => {} };
		},
		setTimeout: fn => timers.push(fn),
		clearTimeout: () => {},
		addEventListener: (name, fn) => {
			listeners[name] = fn;
		},
		Date,
		JSON,
		Math,
		Uint8Array,
		Blob: class {},
	};
	sandbox.window = sandbox;
	sandbox.globalThis = sandbox;
	// An embedder, when the test wants one. `parent === window` is how a page knows it is
	// NOT framed, which is the browser's own convention.
	sandbox.parent = framed ? { postMessage: (m, o) => posted.push([m, o]) } : sandbox;

	createContext(sandbox);
	runInContext(SOURCE, sandbox);

	return {
		mlx: (...args) => sandbox.window.mlx(...args),
		/** Let every pending timer run, which is what makes a batch go out. */
		flush() {
			while (timers.length) timers.shift()();
		},
		/** Navigate the way a single-page app does, without a document load. */
		goto(newPath, newSearch = '') {
			location.pathname = newPath;
			location.search = newSearch;
			location.href = 'https://site.example' + newPath + newSearch;
		},
		/** Every event sent, flattened out of its envelopes. */
		events() {
			return sent.flatMap(b => b.b);
		},
		views() {
			return sent.flatMap(b => b.b).filter(e => e.e === 'pageview');
		},
		envelopes: sent,
		/** Everything posted to the embedder. */
		posted,
	};
}

test('counts the arrival', () => {
	const page = load();
	page.flush();
	assert.equal(page.views().length, 1);
});

test('a query-string change is not a second view of the same page', () => {
	// The stored path has no query — the server keeps EscapedPath() and reads the query only
	// for campaign parameters. So two views here would be two views of ONE row, which is
	// exactly the inflation that made the dashboard disagree with itself.
	const page = load({ path: '/pricing', search: '?plan=pro' });
	page.goto('/pricing', '?plan=lite');
	page.mlx('page', 'pricing');
	page.flush();

	assert.equal(page.views().length, 1);
});

test('two pages served at one address are two views', () => {
	// What A/B page routing does. Without the page name in the key the second arm was never
	// counted at all, and the split looked like it had no traffic on one side.
	const page = load({ path: '/' });
	page.mlx('page', 'home');
	page.mlx('page', 'homeTwo');
	page.flush();

	const views = page.views();
	assert.equal(views.length, 2);
	assert.deepEqual(views.map(v => v.g), ['home', 'homeTwo']);
});

test('a page name arriving late names the view instead of adding one', () => {
	// The application knows where it is a few milliseconds after the browser does. Sending a
	// second, named view would double every arrival on every site.
	const page = load({ path: '/' });
	page.mlx('page', 'homeTwo');
	page.flush();

	const views = page.views();
	assert.equal(views.length, 1);
	assert.equal(views[0].g, 'homeTwo');
});

test('the same name arriving twice is not a second view', () => {
	const page = load({ path: '/' });
	page.mlx('page', 'home');
	page.mlx('page', 'home');
	page.flush();

	assert.equal(page.views().length, 1);
});

test('a name that arrives after the batch has gone names only what follows', () => {
	// The honest limit of naming in place: once it is sent it is sent. Two seconds wide in
	// practice, and the view still lands — under the address rather than the page.
	const page = load({ path: '/' });
	page.flush();
	page.mlx('page', 'home');
	page.flush();

	const views = page.views();
	assert.equal(views.length, 2, 'a name after the flush is a new view, because the first is gone');
	assert.equal(views[0].g, undefined);
	assert.equal(views[1].g, 'home');
});

test('navigating to a different path is a new view', () => {
	const page = load({ path: '/' });
	page.mlx('page', 'home');
	page.goto('/pricing');
	page.mlx('page', 'pricing');
	page.flush();

	assert.deepEqual(page.views().map(v => v.g), ['home', 'pricing']);
});

test('consent withheld records nothing, and granting it counts the arrival', () => {
	const page = load({ path: '/', attrs: { 'data-consent': 'required' } });
	page.mlx('page', 'home');
	page.flush();
	assert.equal(page.events().length, 0, 'nothing may be sent before an answer');

	page.mlx('consent', true);
	page.flush();

	const views = page.views();
	assert.equal(views.length, 1);
	assert.equal(views[0].g, 'home', 'the name was known before the answer and must survive it');
});

test('the experiment tag rides on the envelope, not the event', () => {
	const page = load({ path: '/' });
	page.mlx('page', 'homeTwo');
	page.mlx('experiment', 'rule1', 'rule1:homeTwo');
	page.flush();

	assert.equal(page.envelopes.length, 1);
	assert.equal(page.envelopes[0].x, 'rule1');
	assert.equal(page.envelopes[0].n, 'rule1:homeTwo');
});

test('an ordinary visit tells nobody how tall the page is', () => {
	// Only a framed page reports. A page nobody is embedding has no one to tell, and
	// broadcasting anyway would be noise on every site that runs this.
	const page = load({ framed: false });
	page.flush();
	assert.equal(page.posted.length, 0);
});

test('a framed page reports its height to the embedder', () => {
	// The heatmap viewer frames the measured page on another origin and cannot measure it.
	// Without this the frame is a guess — 2400px — and a taller page is cut off part way
	// down, which is exactly what it did on crumbco (4626px).
	const page = load({ framed: true, docHeight: 4626 });
	page.flush();

	assert.ok(page.posted.length > 0, 'a framed page must say how tall it is');
	const [message, target] = page.posted[0];
	// Field by field, not deepEqual: the message is built inside the vm sandbox, so its
	// prototype is that realm's Object and strict deep equality refuses it on identity.
	assert.equal(message.mlx, 'height');
	assert.equal(message.height, 4626);
	// The embedder checks the origin; naming one here would mean knowing where a page is
	// allowed to be framed, which is the site owner's decision and not this script's.
	assert.equal(target, '*');
});

test('it reports the height once, not on every nudge', () => {
	const page = load({ framed: true, docHeight: 4626 });
	page.flush();
	assert.equal(page.posted.length, 1, 'an unchanged height is not news');
});
