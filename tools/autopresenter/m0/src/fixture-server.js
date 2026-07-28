"use strict";

const fs = require("node:fs");
const http = require("node:http");
const path = require("node:path");
const { ContractError } = require("./errors");

const LOOPBACK_HOST = "127.0.0.1";

function fixtureResponse(fixtureRoot, requestPath) {
  if (requestPath === "/" || requestPath === "/index.html") {
    return {
      body: fs.readFileSync(path.join(fixtureRoot, "index.html")),
      status: 200,
      type: "text/html; charset=utf-8",
    };
  }
  if (requestPath === "/zavtra/" || requestPath === "/zavtra/index.html") {
    return {
      body: fs.readFileSync(path.join(fixtureRoot, "zavtra", "index.html")),
      status: 200,
      type: "text/html; charset=utf-8",
    };
  }
  if (requestPath === "/healthz") {
    return {
      body: Buffer.from("ok\n"),
      status: 200,
      type: "text/plain; charset=utf-8",
    };
  }
  if (requestPath === "/favicon.ico") {
    return { body: Buffer.alloc(0), status: 204, type: "image/x-icon" };
  }
  return {
    body: Buffer.from("not found\n"),
    status: 404,
    type: "text/plain; charset=utf-8",
  };
}

async function startFixtureServer(fixtureRoot) {
  const realFixtureRoot = fs.realpathSync.native(fixtureRoot);
  for (const relative of ["index.html", path.join("zavtra", "index.html")]) {
    if (!fs.statSync(path.join(realFixtureRoot, relative)).isFile()) {
      throw new ContractError(
        "FIXTURE_INCOMPLETE",
        `Missing deterministic fixture file: ${relative}`,
      );
    }
  }

  const server = http.createServer((request, response) => {
    try {
      const url = new URL(request.url, `http://${LOOPBACK_HOST}`);
      const payload = fixtureResponse(realFixtureRoot, url.pathname);
      response.writeHead(payload.status, {
        "Cache-Control": "no-store",
        "Content-Length": payload.body.length,
        "Content-Security-Policy":
          "default-src 'none'; style-src 'unsafe-inline'; base-uri 'none'; form-action 'none'",
        "Content-Type": payload.type,
        "Referrer-Policy": "no-referrer",
        "X-Content-Type-Options": "nosniff",
      });
      response.end(payload.body);
    } catch {
      response.writeHead(500, { "Content-Type": "text/plain; charset=utf-8" });
      response.end("fixture error\n");
    }
  });
  server.keepAliveTimeout = 1000;
  server.headersTimeout = 2000;

  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen({ host: LOOPBACK_HOST, port: 0, exclusive: true }, resolve);
  });
  const address = server.address();
  if (!address || typeof address === "string" || address.address !== LOOPBACK_HOST) {
    server.close();
    throw new ContractError(
      "FIXTURE_NOT_LOOPBACK",
      "Fixture server did not bind to the required IPv4 loopback interface",
    );
  }

  let closed = false;
  return {
    host: LOOPBACK_HOST,
    port: address.port,
    rootUrl: `http://${LOOPBACK_HOST}:${address.port}/`,
    async close() {
      if (closed) {
        return;
      }
      closed = true;
      server.closeAllConnections?.();
      await new Promise((resolve, reject) => {
        server.close((error) => (error ? reject(error) : resolve()));
      });
    },
  };
}

module.exports = {
  LOOPBACK_HOST,
  startFixtureServer,
};
