/**
 * ACC Frontend Server — static files + API reverse proxy
 *
 * Serves dist/ on port 3010 and proxies /api/* and /ws to backend on 8000.
 * Replaces `serve -s dist` which has no proxy support.
 */
const http = require("http");
const fs = require("fs");
const path = require("path");
const url = require("url");

const PORT = 3010;
const BACKEND = "http://127.0.0.1:8000";
const DIST = path.join(__dirname, "dist");

const MIME_TYPES = {
  ".html": "text/html; charset=utf-8",
  ".js": "application/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".json": "application/json",
  ".svg": "image/svg+xml",
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".ico": "image/x-icon",
  ".woff": "font/woff",
  ".woff2": "font/woff2",
  ".ttf": "font/ttf",
  ".map": "application/json",
};

function proxyRequest(req, res) {
  const parsed = url.parse(BACKEND + req.url);
  const opts = {
    hostname: parsed.hostname,
    port: parsed.port,
    path: parsed.path,
    method: req.method,
    headers: { ...req.headers, host: parsed.host },
  };

  const proxyReq = http.request(opts, (proxyRes) => {
    res.writeHead(proxyRes.statusCode, proxyRes.headers);
    proxyRes.pipe(res, { end: true });
  });

  proxyReq.on("error", (err) => {
    console.error(`Proxy error: ${err.message}`);
    res.writeHead(502, { "Content-Type": "text/plain" });
    res.end("Backend unavailable");
  });

  req.pipe(proxyReq, { end: true });
}

function serveStatic(req, res) {
  let filePath = path.join(DIST, url.parse(req.url).pathname);

  // If path is a directory, try index.html
  try {
    if (fs.statSync(filePath).isDirectory()) {
      filePath = path.join(filePath, "index.html");
    }
  } catch {}

  // Try serve the file
  if (fs.existsSync(filePath) && fs.statSync(filePath).isFile()) {
    const ext = path.extname(filePath).toLowerCase();
    const mime = MIME_TYPES[ext] || "application/octet-stream";

    // Cache assets (hashed filenames) for 1 year, HTML never cache
    const cacheControl = ext === ".html"
      ? "no-cache"
      : "public, max-age=31536000, immutable";

    res.writeHead(200, {
      "Content-Type": mime,
      "Cache-Control": cacheControl,
    });
    fs.createReadStream(filePath).pipe(res);
  } else {
    // SPA fallback — serve index.html for all unknown routes
    const indexPath = path.join(DIST, "index.html");
    if (fs.existsSync(indexPath)) {
      res.writeHead(200, {
        "Content-Type": "text/html; charset=utf-8",
        "Cache-Control": "no-cache",
      });
      fs.createReadStream(indexPath).pipe(res);
    } else {
      res.writeHead(404, { "Content-Type": "text/plain" });
      res.end("Not Found");
    }
  }
}

const server = http.createServer((req, res) => {
  // Proxy: /api/* and /ws to backend
  if (req.url.startsWith("/api") || req.url.startsWith("/ws")) {
    proxyRequest(req, res);
  } else {
    serveStatic(req, res);
  }
});

// Handle WebSocket upgrade for /ws
server.on("upgrade", (req, socket, head) => {
  if (!req.url.startsWith("/ws")) {
    socket.destroy();
    return;
  }

  const parsed = url.parse(BACKEND + req.url);
  const opts = {
    hostname: parsed.hostname,
    port: parsed.port,
    path: parsed.path,
    method: "GET",
    headers: { ...req.headers, host: parsed.host },
  };

  const proxyReq = http.request(opts);
  proxyReq.on("upgrade", (proxyRes, proxySocket, proxyHead) => {
    socket.write(
      `HTTP/1.1 101 Switching Protocols\r\n` +
      Object.entries(proxyRes.headers)
        .map(([k, v]) => `${k}: ${v}`)
        .join("\r\n") +
      "\r\n\r\n"
    );
    if (proxyHead.length) socket.write(proxyHead);
    proxySocket.pipe(socket);
    socket.pipe(proxySocket);
  });

  proxyReq.on("error", (err) => {
    console.error(`WS proxy error: ${err.message}`);
    socket.destroy();
  });

  proxyReq.end();
});

server.listen(PORT, "0.0.0.0", () => {
  console.log(`ACC Frontend listening on http://0.0.0.0:${PORT}`);
  console.log(`  Static: ${DIST}`);
  console.log(`  Proxy:  /api/* /ws → ${BACKEND}`);
});
