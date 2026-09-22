/*
 * The Modlix analytics beacon.
 *
 * Served by the engine that receives its events, so there is one copy of this file rather
 * than one per page-generating service. The two snippet generators (IndexHTMLService for
 * CSR, htmlRenderer for SSR) each emitted their own transcription of the previous vendor's
 * stub, and the two had already begun to drift.
 *
 * Configuration comes from the script tag's own data attributes, and the endpoint from its
 * own src — so a page names the host once.
 *
 *   <script async src="https://analytics.example/a.js"
 *           data-autocapture="true" data-pageviews="true"
 *           data-pageleaves="true" data-heatmaps="false"
 *           data-consent="required"></script>
 *
 * Public surface, through the queue stub the snippet installs as window.mlx:
 *
 *   mlx('capture', 'checkout_started', {plan: 'pro'})
 *   mlx('page', 'checkoutPage')      // the host application's page identity
 *   mlx('experiment', 'pricing', 'b')
 *   mlx('consent', true)             // false revokes and stops everything
 */
(function () {
	'use strict';

	var doc = document;
	var script = doc.currentScript;
	if (!script) {
		var all = doc.getElementsByTagName('script');
		for (var i = all.length - 1; i >= 0; i--) {
			if (all[i].src && all[i].src.indexOf('/a.js') > 0) {
				script = all[i];
				break;
			}
		}
	}
	if (!script) return;

	// The endpoint is the script's own origin. One place names the host: the tag.
	var endpoint = script.src.replace(/\/a\.js.*$/, '') + '/i';

	function flag(name, dflt) {
		var v = script.getAttribute('data-' + name);
		if (v === null) return dflt;
		return v !== 'false' && v !== '0';
	}

	var wantPageviews = flag('pageviews', true);
	var wantPageleaves = flag('pageleaves', true);
	var wantAutocapture = flag('autocapture', true);
	// Off unless the app asks. Every click on the page is one event, where autocapture is
	// only the labelled ones — so this multiplies an app's event volume by however clicky its
	// pages are, and that is a decision somebody should make on purpose.
	var wantHeatmaps = flag('heatmaps', false);
	var consentRequired = script.getAttribute('data-consent') === 'required';

	// Opted out until told otherwise, whenever consent is required. The safe direction:
	// a page that fails to ask has measured nobody, rather than everybody.
	var allowed = !consentRequired;

	/**
	 * Being LOOKED AT rather than visited.
	 *
	 * The heatmap viewer frames a page to draw its clicks over it, and asks for that page
	 * as-is with `modlixDesign` so that page routing does not serve a different arm. A page
	 * opened that way is somebody reading their own numbers, and counting it would add to
	 * the very figures they are reading — a page would gain a view every time anyone looked
	 * at its heatmap, and the busiest page would be the one most often inspected.
	 *
	 * Read from the URL, not from a message: the frame is already loading by the time a
	 * message could arrive, and the first page view is the one that matters.
	 */
	var design = /[?&]modlixDesign=(?!false|0)/.test(location.search);

	var pageName = script.getAttribute('data-page') || '';
	var experiment = '';
	var variant = '';

	/* ---- session ---------------------------------------------------------------- */

	// A session id, and deliberately NOT a visitor id: the engine derives the visitor
	// itself from a daily-rotating salt, so there is no durable identifier in this page
	// and nothing to consent to storing beyond the tab's own lifetime.
	var SESSION_KEY = 'mlx_s';
	var IDLE_MS = 30 * 60 * 1000;

	function now() {
		return Date.now();
	}

	function randomId() {
		try {
			var b = new Uint8Array(16);
			crypto.getRandomValues(b);
			var s = '';
			for (var i = 0; i < b.length; i++) s += ('0' + b[i].toString(16)).slice(-2);
			return s;
		} catch (e) {
			return String(now()) + Math.random().toString(16).slice(2);
		}
	}

	function session() {
		// sessionStorage can throw outright in a partitioned or locked-down context, so
		// every access is guarded and a failure degrades to a per-page-load session
		// rather than to no analytics at all.
		try {
			var raw = sessionStorage.getItem(SESSION_KEY);
			var parsed = raw ? JSON.parse(raw) : null;
			if (parsed && parsed.id && now() - parsed.at < IDLE_MS) {
				parsed.at = now();
				sessionStorage.setItem(SESSION_KEY, JSON.stringify(parsed));
				return parsed.id;
			}
			var fresh = { id: randomId(), at: now() };
			sessionStorage.setItem(SESSION_KEY, JSON.stringify(fresh));
			return fresh.id;
		} catch (e) {
			if (!session.fallback) session.fallback = randomId();
			return session.fallback;
		}
	}

	/* ---- sending ---------------------------------------------------------------- */

	var queue = [];
	var timer = null;
	var MAX_BATCH = 20;
	var LINGER_MS = 2000;

	function envelope(events) {
		var body = {
			u: location.href,
			r: doc.referrer,
			s: session(),
			b: events,
		};
		if (experiment) body.x = experiment;
		if (variant) body.n = variant;
		return JSON.stringify(body);
	}

	function send(events, beacon) {
		if (!events.length) return;
		var payload = envelope(events);

		// text/plain, not application/json: a JSON content type makes this a non-simple
		// cross-origin request and costs a preflight round trip on every page. The
		// engine parses the body regardless of what the header claims.
		try {
			if (beacon && navigator.sendBeacon) {
				navigator.sendBeacon(endpoint, new Blob([payload], { type: 'text/plain' }));
				return;
			}
			fetch(endpoint, {
				method: 'POST',
				body: payload,
				headers: { 'Content-Type': 'text/plain' },
				// Survives the page being torn down mid-flight, which is exactly when
				// the last and most interesting events are sent.
				keepalive: true,
				// Nothing here reads the response, and no cookie should ride along.
				credentials: 'omit',
				mode: 'cors',
			}).catch(function () {});
		} catch (e) {
			/* Analytics must never break the page it measures. */
		}
	}

	function flush(beacon) {
		if (timer) {
			clearTimeout(timer);
			timer = null;
		}
		var batch = queue;
		queue = [];
		send(batch, beacon);
	}

	function record(name, props, label, where) {
		if (design || !allowed || !name) return;

		var e = { e: name, t: now() };
		if (label) e.l = label;
		if (where) {
			e.x = where.x;
			e.y = where.y;
			e.w = where.w;
		}
		if (pageName) e.g = pageName;
		// The URL of THIS event, which in a single-page app is not the URL the batch was
		// opened with.
		e.u = location.href;
		if (props && typeof props === 'object') {
			try {
				e.p = JSON.stringify(props);
			} catch (err) {
				/* A property bag that will not serialise is dropped, the event is not. */
			}
		}

		queue.push(e);
		if (queue.length >= MAX_BATCH) flush(false);
		else if (!timer) timer = setTimeout(function () { flush(false); }, LINGER_MS);
	}

	/* ---- page views -------------------------------------------------------------- */

	var lastView = null;

	/**
	 * What counts as "the same view".
	 *
	 * The path WITHOUT the query, because that is exactly what is stored: the server
	 * keeps `EscapedPath()` and mines the query only for campaign parameters. Keying
	 * on the query as well counted a filter change as a second view of one page --
	 * which the comment here had always said must not happen, while the code did it.
	 *
	 * And the page NAME, because an application can serve two different pages at one
	 * address. A/B page routing does exactly that, and without the name the second
	 * arm was never counted as a view at all.
	 */
	function viewKey() {
		return location.pathname + '\n' + pageName;
	}

	function pageview() {
		if (!wantPageviews) return;
		var here = viewKey();
		if (here === lastView) return;
		lastView = here;
		record('pageview', null, null);
	}

	/**
	 * The page name arriving after the view it belongs to.
	 *
	 * An application knows where it is some milliseconds after the browser does, so
	 * the arrival view is often recorded before `mlx('page', ...)` — and on a site
	 * that asks for consent, always: the view is held until the visitor answers, and
	 * the answer can reach us first. Sending a second, named view would double every
	 * arrival. Naming the one already queued is not a second event: it has not been
	 * sent. Only if the batch has already flushed is the name lost, and that is a
	 * two-second window.
	 */
	function nameQueuedViews() {
		var named = false;
		for (var i = 0; i < queue.length; i++) {
			if (queue[i].e === 'pageview' && !queue[i].g) {
				queue[i].g = pageName;
				named = true;
			}
		}
		// The view it belongs to is accounted for, so `pageview()` must not treat the
		// new name as a new view.
		if (named) lastView = viewKey();
	}

	function watchNavigation() {
		var wrap = function (name) {
			var original = history[name];
			if (typeof original !== 'function') return;
			history[name] = function () {
				var out = original.apply(this, arguments);
				// After the frameworks own handlers, so location is already the new one.
				setTimeout(pageview, 0);
				return out;
			};
		};
		wrap('pushState');
		wrap('replaceState');
		addEventListener('popstate', function () { setTimeout(pageview, 0); });
	}

	/* ---- autocapture -------------------------------------------------------------- */

	// Two different things ride on one listener, and they are deliberately separate events.
	//
	// `click` is autocapture: only elements carrying data-analytics-label, which is what the
	// platform's `analyticsLabel` component property emits. Narrower than the vendor default
	// of capturing every click and deriving a name from the DOM — those names change whenever
	// the markup does, so a funnel built on them breaks on a redesign with no error, and the
	// text of a clicked element can carry a person's own data into an event name. A label is
	// a decision somebody made.
	//
	// `$click` is the heatmap's raw material: every click anywhere, carrying where it landed
	// and nothing about what it hit. Keeping them apart means switching heatmaps on cannot
	// change what the funnel or the event list say.
	function watchClicks() {
		if (!wantAutocapture && !wantHeatmaps) return;

		doc.addEventListener(
			'click',
			function (ev) {
				if (wantHeatmaps) record('$click', null, null, positionOf(ev));
				if (!wantAutocapture) return;

				var el = ev.target;
				while (el && el !== doc.body) {
					if (el.getAttribute) {
						var label = el.getAttribute('data-analytics-label');
						if (label) {
							record('click', null, label);
							return;
						}
					}
					el = el.parentNode;
				}
			},
			true,
		);
	}

	/**
	 * Where a click landed, in the only units that survive being looked at later.
	 *
	 * x is a PROPORTION of the viewport width, in ten-thousandths, because a pixel abscissa
	 * means the middle of a phone and the left gutter of a desktop — averaging the two draws a
	 * picture of nowhere. y is absolute document pixels, because vertical position does not
	 * scale with width: a header is a header at any size. The width itself travels too, so the
	 * reader can band by layout rather than pretending every screen is the same.
	 *
	 * Uses pageX/pageY, which already include the scroll offset. clientY would put every click
	 * in the top screenful of the page, and the map would look plausible.
	 */
	function positionOf(ev) {
		var w = window.innerWidth || doc.documentElement.clientWidth || 0;
		if (!w) return null;

		var x = ev.pageX;
		var y = ev.pageY;
		if (typeof x !== 'number' || typeof y !== 'number') return null;

		return {
			x: Math.max(0, Math.min(10000, Math.round((x / w) * 10000))),
			y: Math.max(0, Math.round(y)),
			w: Math.round(w),
		};
	}

	/* ---- being looked at ----------------------------------------------------------- */

	/**
	 * Tell an embedder how tall this document is.
	 *
	 * The heatmap viewer frames the measured page and draws clicks over it on a canvas of its
	 * own. It has to make the frame as tall as the whole page: a frame that scrolls slides the
	 * page out from under a canvas that cannot follow, and every blob then sits on the wrong
	 * thing. But it cannot MEASURE the page — it is another origin, and that is the whole
	 * point of the same-origin policy.
	 *
	 * So the page says. This is the one script that is already on both sides of that boundary.
	 * Height is the only thing sent, it is the document's own layout rather than anything
	 * about the person reading it, and it is sent only when actually framed — so an ordinary
	 * visit posts nothing at all. Deliberately NOT gated on consent: there is nothing here to
	 * consent to, and an unanswered banner would otherwise leave the viewer with a guess.
	 */
	function reportHeightWhenFramed() {
		var parent = window.parent;
		if (!parent || parent === window) return;

		var last = 0;
		var tell = function () {
			var d = doc.documentElement;
			var h = Math.max(
				d ? d.scrollHeight : 0,
				doc.body ? doc.body.scrollHeight : 0,
			);
			if (!h || h === last) return;
			last = h;
			try {
				// The embedder checks the origin; naming it here would mean knowing it, and a
				// page can be framed from anywhere it has allowed.
				parent.postMessage({ mlx: 'height', height: h }, '*');
			} catch (e) {
				/* Analytics must never break the page it measures. */
			}
		};

		tell();
		addEventListener('load', tell);
		addEventListener('resize', tell);
		// A page grows after it arrives: fonts land, images decode, a consent bar is answered
		// and disappears. A ResizeObserver on the root catches all of it in one line, and the
		// timers cover the browsers that have none.
		if (typeof ResizeObserver === 'function' && doc.documentElement) {
			try {
				new ResizeObserver(tell).observe(doc.documentElement);
			} catch (e) {
				/* fall through to the timers */
			}
		}
		setTimeout(tell, 500);
		setTimeout(tell, 2000);
	}

	/* ---- the public queue ---------------------------------------------------------- */

	function handle(args) {
		var op = args[0];
		if (op === 'capture') record(args[1], args[2], null);
		else if (op === 'page') {
			pageName = args[1] || '';
			// Name the view this belongs to before deciding whether there is a new one:
			// an app that has just told us where we are is describing the view already
			// recorded, not announcing another.
			nameQueuedViews();
			// A page identity arriving after load means the app has just told us where
			// we are, which is the first moment a page view is meaningful.
			pageview();
		} else if (op === 'experiment') {
			experiment = args[1] || '';
			variant = args[2] || '';
		} else if (op === 'consent') {
			var granted = !!args[1];
			if (granted && !allowed) {
				allowed = true;
				// The view that was refused before consent. Without this, a visitor who
				// accepts on arrival is never counted as having arrived.
				lastView = null;
				pageview();
			} else if (!granted) {
				allowed = false;
				queue = [];
			}
		}
	}

	var pending = (window.mlx && window.mlx.q) || [];
	window.mlx = function () {
		handle(arguments);
	};
	for (var j = 0; j < pending.length; j++) handle(pending[j]);

	watchNavigation();
	watchClicks();
	reportHeightWhenFramed();
	pageview();

	addEventListener(
		'pagehide',
		function () {
			if (wantPageleaves) record('pageleave', null, null);
			flush(true);
		},
		{ capture: true },
	);

	// Firefox does not fire pagehide on a background tab being discarded; this one it does.
	addEventListener('visibilitychange', function () {
		if (doc.visibilityState === 'hidden') flush(true);
	});
})();
