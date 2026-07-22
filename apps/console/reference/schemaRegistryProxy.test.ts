import { describe, expect, it } from "vitest";
import { registryBaseUrl, upstreamPath } from "./schemaRegistryProxy.js";

describe("registryBaseUrl", () => {
    it("defaults to the local dev registry", () => {
        expect(registryBaseUrl({}).origin).toBe("http://localhost:8081");
    });

    it("honors PROTOMOLT_REGISTRY_URL", () => {
        const base = registryBaseUrl({ PROTOMOLT_REGISTRY_URL: "http://registry.internal:9090" });
        expect(base.hostname).toBe("registry.internal");
        expect(base.port).toBe("9090");
    });
});

describe("upstreamPath", () => {
    const base = new URL("http://localhost:8081");

    it("passes the remainder through unchanged", () => {
        expect(upstreamPath(base, "/subjects")).toBe("/subjects");
        expect(upstreamPath(base, "/config?x=1")).toBe("/config?x=1");
    });

    it("preserves percent-encoded slashes in subject segments", () => {
        expect(upstreamPath(base, "/subjects/example%2Fperson.proto/versions/latest"))
            .toBe("/subjects/example%2Fperson.proto/versions/latest");
    });

    it("prefixes a base path when the registry is mounted under one", () => {
        const mounted = new URL("http://registry.internal:9090/registry/");
        expect(upstreamPath(mounted, "/subjects")).toBe("/registry/subjects");
    });
});
