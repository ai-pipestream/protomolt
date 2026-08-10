/**
 * Structured generation: fill one protobuf message with a model. The
 * coordinator renders the prompt packet for the target type, calls the
 * inference catalog, parses the response as strict protobuf JSON, validates
 * it against the type's declared rules, and retries only from the rendered
 * rejection feedback, never exceeding three attempts.
 */
package ai.pipestream.proto.inference.structured;
