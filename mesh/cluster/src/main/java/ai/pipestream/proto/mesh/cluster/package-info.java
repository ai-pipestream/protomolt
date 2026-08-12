/**
 * The mesh cluster directory: the in-memory, TTL-driven advertisement and presence directory
 * ({@link ai.pipestream.proto.mesh.cluster.ClusterDirectory}), its fail-fast validation layer
 * ({@link ai.pipestream.proto.mesh.cluster.ClusterValidation}), and the pure delegation bridge
 * ({@link ai.pipestream.proto.mesh.cluster.DelegationBridge}) over the wire types generated from
 * this module's {@code cluster.proto}. In-process only: no networking, storage, or threads; time
 * comes from an injected clock.
 */
package ai.pipestream.proto.mesh.cluster;
