/**
 * CEL function declarations and bindings for the format predicates custom validation rules use.
 *
 * <p>{@link ai.protomolt.proto.validate.cel.ValidationCelFunctions} supplies the member-call
 * functions protovalidate expects on the receiver type — {@code this.isHostname()},
 * {@code this.isIp(4)}, {@code this.isUri()} and the rest — as a list of declarations for the CEL
 * compiler and a matching list of bindings for the CEL runtime. Both are registered into the
 * validation environment built by
 * {@link ai.protomolt.proto.cel.CelEnvironmentFactory}.
 *
 * <p>The semantics come from the dependency-free {@link ai.protomolt.proto.formats.Formats} validators,
 * so the same RFC logic backs these functions and the well-known formats declared through
 * {@link ai.protomolt.proto.validate.model.StringFormat}. Custom CEL rules reach this package
 * indirectly: {@link ai.protomolt.proto.validate.ProtoValidator} installs the functions before
 * compiling any {@link ai.protomolt.proto.validate.model.CelConstraint}.
 */
package ai.protomolt.proto.validate.cel;
