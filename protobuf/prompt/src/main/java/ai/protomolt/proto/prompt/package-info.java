/**
 * Prompt packets: render a protobuf descriptor into the complete briefing for a model
 * asked to fill the form ({@link ai.protomolt.proto.prompt.PromptRenderer}), shape the
 * decoder constraint for serving stacks
 * ({@link ai.protomolt.proto.prompt.ResponseFormatShaper}), and render validation
 * rejections as retry feedback
 * ({@link ai.protomolt.proto.prompt.ViolationFeedbackRenderer}). Pure functions over
 * descriptors — no registry I/O, no model calls.
 */
package ai.protomolt.proto.prompt;
