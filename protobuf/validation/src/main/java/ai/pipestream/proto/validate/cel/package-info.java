/**
 * CEL function declarations and bindings for the format predicates custom validation rules use.
 *
 * <p>{@link ai.pipestream.proto.validate.cel.ValidationCelFunctions} supplies the member-call
 * functions protovalidate expects on the receiver type — {@code this.isHostname()},
 * {@code this.isIp(4)}, {@code this.isUri()} and the rest — as a list of declarations for the CEL
 * compiler and a matching list of bindings for the CEL runtime. Both are registered into the
 * validation environment built by
 * {@link ai.pipestream.proto.cel.CelEnvironmentFactory}.
 *
 * <p>The semantics come from the dependency-free {@link ai.pipestream.format.Formats} validators,
 * so the same RFC logic backs these functions and the well-known formats declared through
 * {@link ai.pipestream.proto.validate.model.StringFormat}. Custom CEL rules reach this package
 * indirectly: {@link ai.pipestream.proto.validate.ProtoValidator} installs the functions before
 * compiling any {@link ai.pipestream.proto.validate.model.CelConstraint}.
 */
package ai.pipestream.proto.validate.cel;
