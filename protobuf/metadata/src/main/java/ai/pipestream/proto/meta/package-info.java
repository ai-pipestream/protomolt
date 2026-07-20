/**
 * Descriptive and operational metadata declared on descriptors as protobuf options.
 *
 * <p>The standard defines {@code FieldOptions} and {@code MessageOptions} extensions under
 * {@code ai.pipestream.proto.meta.v1} — ownership, sensitivity class, descriptions, display
 * names, labels — generated into this package as {@link FieldMeta} and {@link MessageMeta}.
 * {@link DescriptorMetadata} reads them back at runtime and flattens them into a bag suitable
 * for logs, headers, or CEL; descriptor sets parsed at runtime need
 * {@link DescriptorMetadata#registerExtensions} called first, or the options stay unknown
 * fields. It also materializes annotation-carried JSON names back onto fields, which is what
 * keeps document keys stable across encoders that drop {@code json_name}.</p>
 *
 * <p>{@link SensitivityMasker} acts on the declared sensitivity class: a field marked once in
 * the contract is removed, redacted, or sealed as AES-GCM ciphertext on every surface that
 * masks. The walk covers nested messages, repeated fields, and message-valued map entries, and
 * {@link SensitivityMasker.PayloadResolver} is the extension point for opening
 * {@code google.protobuf.Any} payloads whose type does not travel with the schema. Payloads it
 * cannot resolve are named in {@link SensitivityMasker.MaskResult#unresolvedPaths()} rather
 * than passed over silently.</p>
 *
 * <p>Metadata declared here is descriptive and travels with the descriptor. Metadata extracted
 * from message contents with CEL selectors is a separate concern and lives in the
 * {@code protomolt-metadata} module.</p>
 *
 * <p>See the <a href="https://github.com/ai-pipestream/protomolt/blob/main/docs/metadata.md">Schema
 * metadata guide</a> for the annotation form and the masking surfaces.</p>
 */
package ai.pipestream.proto.meta;
