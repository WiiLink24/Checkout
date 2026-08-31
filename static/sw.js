/* WiiLink Checkout service worker.
   Caches static assets only — never pages, APIs or anything user-specific. */
const CACHE_NAME = "checkout-static-v1";
const OFFLINE_URL = "/static/offline.html";

self.addEventListener("install", (event) => {
    event.waitUntil(
        caches
            .open(CACHE_NAME)
            .then((cache) =>
                cache.addAll(["/static/manifest.webmanifest", "/static/img/logo.png", OFFLINE_URL])
            )
            .then(() => self.skipWaiting())
    );
});

self.addEventListener("activate", (event) => {
    event.waitUntil(
        caches
            .keys()
            .then((keys) =>
                Promise.all(
                    keys.filter((key) => key !== CACHE_NAME).map((key) => caches.delete(key))
                )
            )
            .then(() => self.clients.claim())
    );
});

self.addEventListener("fetch", (event) => {
    const url = new URL(event.request.url);
    if (event.request.method !== "GET" || url.origin !== self.location.origin) return;

    // Offline fallback for page navigations (only when the network is unreachable).
    if (event.request.mode === "navigate") {
        event.respondWith(
            fetch(event.request).catch(() => caches.match(OFFLINE_URL))
        );
        return;
    }

    if (!url.pathname.startsWith("/static/")) return; // never cache pages/API/user data

    // Network-first with cache fallback: fresh assets when online, offline-capable otherwise.
    event.respondWith(
        fetch(event.request)
            .then((response) => {
                if (response && response.ok) {
                    const copy = response.clone();
                    caches.open(CACHE_NAME).then((cache) => cache.put(event.request, copy));
                }
                return response;
            })
            .catch(() => caches.match(event.request))
    );
});
