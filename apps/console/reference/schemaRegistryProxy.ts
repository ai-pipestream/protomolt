/**
 * Reverse proxy to the ProtoMolt schema registry (Confluent subjects
 * protocol over plain HTTP). The browser talks same-origin to
 * `/api/protomolt/*`; this pipes the request to the registry server
 * configured via PROTOMOLT_REGISTRY_URL (default http://localhost:8081).
 *
 * Plain HTTP piping — the registry serves both JSON envelopes and binary
 * FileDescriptorSets (`application/x-protobuf`), so bodies must flow
 * through untouched. Subject names may contain URL-encoded slashes
 * (`%2F`); Express keeps `req.url` raw after the mount prefix, so the
 * encoding survives the hop.
 */
import type { Express, Request, Response } from 'express';
import http from 'node:http';

export const SCHEMA_REGISTRY_PROXY_PREFIX = '/api/protomolt';

/** Registry base URL from the environment (default local dev registry). */
export function registryBaseUrl(env: NodeJS.ProcessEnv = process.env): URL {
    return new URL(env.PROTOMOLT_REGISTRY_URL || 'http://localhost:8081');
}

/**
 * Upstream path for a proxied request: the base URL's path (if any, minus a
 * trailing slash) plus the raw remainder after the mount prefix. Pure so the
 * encoding contract (`%2F` subjects survive) is unit-testable.
 */
export function upstreamPath(base: URL, rawRemainder: string): string {
    const basePath = base.pathname.replace(/\/+$/, '');
    return `${basePath}${rawRemainder.startsWith('/') ? '' : '/'}${rawRemainder}`;
}

export function registerSchemaRegistryProxy(app: Express, env: NodeJS.ProcessEnv = process.env): void {
    app.use(SCHEMA_REGISTRY_PROXY_PREFIX, (req: Request, res: Response) => {
        const base = registryBaseUrl(env);
        const upstream = http.request({
            host: base.hostname,
            port: base.port || 80,
            method: req.method,
            path: upstreamPath(base, req.url),
            headers: { ...req.headers, host: base.host },
        }, (upstreamRes) => {
            res.writeHead(upstreamRes.statusCode ?? 502, upstreamRes.headers);
            upstreamRes.pipe(res);
        });
        upstream.on('error', (err) => {
            // Same envelope shape the registry uses, so the UI's error
            // rendering handles "registry down" like any other failure.
            if (!res.headersSent) {
                res.status(503).json({
                    error_code: 50301,
                    message: `ProtoMolt registry unreachable at ${base.origin}: ${String(err)}`,
                });
            } else {
                res.end();
            }
        });
        req.pipe(upstream);
    });
}
