/**
 * The core every pull connector shares. A pull connector reads a source of record (a bucket, a
 * database) and feeds what changed through the intake service — never into repo-service directly,
 * so account identity keeps riding the API key and every intake rule (scope narrowing, payload
 * caps, save shape) applies to pulled documents exactly as to pushed ones.
 *
 * <p>{@link ai.protomolt.proto.acquire.pull.IntakeFeed} is the submission seam;
 * {@link ai.protomolt.proto.acquire.pull.PullDocuments} wraps a source item into a
 * stable-identity document (same source item, same doc id, so an updated item replaces rather
 * than duplicates); {@link ai.protomolt.proto.acquire.pull.PullReport} is the watermark-carrying
 * outcome. Connectors are stateless: the watermark travels in and out of every pull, owned by
 * the caller — stop is pause, and there is no hidden cursor to leak.</p>
 */
package ai.protomolt.proto.acquire.pull;
