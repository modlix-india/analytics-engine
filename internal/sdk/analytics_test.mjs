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
function load({ path = '/', search = '', attrs = {}, framed = false, docHeight = 4626, viewHeight = 900 } = {}) {
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
			// Document-level listeners, so a test can dispatch a submit the way a form does.
			_docListeners: {},
			addEventListener: (name, fn) => {
				sandbox.document._docListeners[name] = fn;
			},
			documentElement: {
				clientWidth: 1440, clientHeight: viewHeight,
				scrollHeight: docHeight, offsetHeight: docHeight, scrollTop: 0,
			},
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
		innerWidth: 1440,
		innerHeight: viewHeight,
		pageYOffset: 0,
		// Runs the callback at once. The beacon coalesces scroll measurements onto a frame,
		// and a test that had to wait for one would be testing the scheduler.
		requestAnimationFrame: fn => fn(),
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
		/** Scroll the window, the way a person does. */
		scrollTo(y) {
			sandbox.pageYOffset = y;
			sandbox.document.documentElement.scrollTop = y;
			if (listeners.scroll) listeners.scroll();
		},
		/** The page grows: an image decodes, a consent bar is answered. */
		setDocHeight(h) {
			sandbox.document.documentElement.scrollHeight = h;
			sandbox.document.documentElement.offsetHeight = h;
			sandbox.document.body.scrollHeight = h;
		},
		/** Submit a form, the way pressing Enter in it does. */
		submit(attrs) {
			const fn = sandbox.document._docListeners.submit;
			if (!fn) throw new Error('nothing is listening for submit');
			fn({ target: { getAttribute: n => (n in attrs ? attrs[n] : null) } });
		},
		/** Leave the document, which is what ends the last view. */
		leave() {
			if (listeners.pagehide) listeners.pagehide();
		},
		scrolls() {
			return sent.flatMap(b => b.b).filter(e => e.e === '$scroll');
		},
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


/* ---- how far down the page people got ---------------------------------------------- */

test('a page that fits on one screen is fully seen without anyone scrolling', () => {
	// The most common page on a small site, and the one a threshold-event implementation
	// reports as 0% — which is the number somebody would then redesign the page on.
	const p = load({ docHeight: 700, viewHeight: 900 });
	p.leave();
	p.flush();

	const [s] = p.scrolls();
	assert.equal(s.d, 100);
});

test('it reports the deepest point reached, not the last one', () => {
	const p = load({ docHeight: 4000, viewHeight: 1000 });

	p.scrollTo(3000);  // bottom of the window at 4000 of 4000 => 100%
	p.scrollTo(0);     // back to the top
	p.leave();
	p.flush();

	const [s] = p.scrolls();
	assert.equal(s.d, 100, 'scrolling back up must not undo having been down');
	assert.equal(s.h, 1000);
	assert.equal(s.dh, 4000);
	assert.equal(s.w, 1440);
});

test('depth is measured to the bottom of the window, not its top', () => {
	const p = load({ docHeight: 4000, viewHeight: 1000 });

	p.scrollTo(1000);  // window covers 1000..2000 of 4000
	p.leave();
	p.flush();

	assert.equal(p.scrolls()[0].d, 50);
});

test('each page of a single-page app reports its own depth', () => {
	const p = load({ path: '/one', docHeight: 4000, viewHeight: 1000 });
	p.mlx('page', 'one');

	p.scrollTo(3000);          // /one: all the way down
	p.goto('/two');
	p.scrollTo(0);             // the router puts the new page at the top
	p.mlx('page', 'two');      // a new view; /one's depth is reported here
	p.leave();                 // /two: only the first screen was ever shown
	p.flush();

	const depths = p.scrolls().map(s => s.d);
	assert.deepEqual(depths, [100, 25], 'the first page was read to the end, the second was not');

	// And each report belongs to the page it describes. `record` stamps the current location,
	// so a report sent a moment too late is filed under the next page.
	const urls = p.scrolls().map(s => s.u);
	assert.ok(urls[0].endsWith('/one'), `first report filed under ${urls[0]}`);
	assert.ok(urls[1].endsWith('/two'), `second report filed under ${urls[1]}`);
});

test('one view reports once, however it ends', () => {
	// A visitor who navigates and then closes the tab must not send two reports for the
	// second page and none for the first.
	const p = load({ path: '/one', docHeight: 2000, viewHeight: 1000 });
	p.leave();
	p.leave();
	p.flush();

	assert.equal(p.scrolls().length, 1);
});

test('a page that grows after it arrives is measured against its real height', () => {
	const p = load({ docHeight: 1000, viewHeight: 1000 });

	// Fully seen, as far as anyone knows at this instant.
	p.setDocHeight(5000);
	p.scrollTo(0);
	p.leave();
	p.flush();

	assert.equal(p.scrolls()[0].d, 20, 'the early 100% must not survive the page growing');
	assert.equal(p.scrolls()[0].dh, 5000);
});

test('scroll can be switched off', () => {
	const p = load({ attrs: { 'data-scroll': 'false' }, docHeight: 4000, viewHeight: 1000 });
	p.scrollTo(2000);
	p.leave();
	p.flush();

	assert.equal(p.scrolls().length, 0);
});

test('a page being looked at in the heatmap viewer reports nothing', () => {
	const p = load({ search: '?modlixDesign=1', docHeight: 4000, viewHeight: 1000 });
	p.scrollTo(3000);
	p.leave();
	p.flush();

	assert.equal(p.scrolls().length, 0);
});

test('the report carries the page name the view was given, even late', () => {
	// The app knows where it is some milliseconds after the browser does, so the name often
	// arrives after the view it belongs to. The scroll report for that view has not been sent
	// yet either, and it has to be filed under the same name the view ended up with — or the
	// depth lands on an unnamed row while the view lands on a named one, and the two can
	// never be read together.
	const p = load({ path: '/', docHeight: 2000, viewHeight: 1000 });
	p.mlx('page', 'homePage');
	p.leave();
	p.flush();

	const [s] = p.scrolls();
	assert.equal(s.g, 'homePage');
	assert.equal(p.views()[0].g, 'homePage');
});


/* ---- forms ------------------------------------------------------------------------- */

test('a submitted form is an event, however it was submitted', () => {
	// The gap this closes: a form sent with the Enter key fires no click, so autocapture saw
	// nothing at all. On a small site the contact form is the conversion.
	const p = load({ attrs: { 'data-autocapture': 'true' } });
	p.submit({ name: 'contact' });
	p.flush();

	const [e] = p.events().filter(e => e.e === 'form_submit');
	assert.equal(e.l, 'contact');
});

test('the form label is a name somebody chose, in order of preference', () => {
	const p = load({ attrs: { 'data-autocapture': 'true' } });
	p.submit({ 'data-analytics-label': 'newsletter', name: 'nl', id: 'x' });
	p.submit({ name: 'enquiry', id: 'y' });
	p.submit({ id: 'quote' });
	p.submit({});
	p.flush();

	const labels = p.events().filter(e => e.e === 'form_submit').map(e => e.l);
	assert.deepEqual(labels, ['newsletter', 'enquiry', 'quote', 'form']);
});

test('a form submission carries nothing but its label', () => {
	// A submit handler is the easiest place in an analytics script to collect an email address
	// by accident, so this asserts the shape of the whole event rather than the absence of one
	// field somebody thought of.
	const p = load({ attrs: { 'data-autocapture': 'true' } });
	p.submit({ name: 'contact', action: '/enquiry?email=someone@example.com' });
	p.flush();

	const [e] = p.events().filter(e => e.e === 'form_submit');
	assert.deepEqual(Object.keys(e).sort(), ['e', 'l', 't', 'u']);
});

test('autocapture off means no form events either', () => {
	const p = load({ attrs: { 'data-autocapture': 'false' } });
	assert.throws(() => p.submit({ name: 'contact' }), /nothing is listening/);
});
