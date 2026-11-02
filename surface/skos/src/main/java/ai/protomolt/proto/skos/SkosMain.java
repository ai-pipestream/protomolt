package ai.protomolt.proto.skos;

import ai.protomolt.proto.types.ConceptScheme;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

/** Explicit file conversion; publishing a scheme remains the config lane's job. */
public final class SkosMain {
    private SkosMain() {}

    /**
     * Converts one file without overwriting an existing output. The file verb supports
     * self-contained schemes; cross-scheme mappings need the library's explicit bindings.
     *
     * @param args import|export, scheme-id, scheme-IRI, turtle|rdfxml, input, output
     * @throws Exception for invalid input, conversion failure or filesystem failure
     */
    public static void main(String[] args) throws Exception {
        if (args.length != 6 || !(args[0].equals("import") || args[0].equals("export")))
            throw new IllegalArgumentException("Usage: import|export scheme-id scheme-IRI turtle|rdfxml input output");
        var format = switch (args[3]) {
            case "turtle" -> SkosConverter.Format.TURTLE;
            case "rdfxml" -> SkosConverter.Format.RDF_XML;
            default -> throw new IllegalArgumentException("Format must be turtle or rdfxml");
        };
        var converter = new SkosConverter(args[1], args[2], Map.of());
        Path input = Path.of(args[4]);
        Path output = Path.of(args[5]).toAbsolutePath();
        Path staged = Files.createTempFile(output.getParent(), ".skos-convert-", ".tmp");
        try {
            try (var source = Files.newInputStream(input); var destination = Files.newOutputStream(staged)) {
                if (args[0].equals("import")) converter.read(source, format).writeTo(destination);
                else converter.write(ConceptScheme.parseFrom(source), destination, format);
            }
            // Without REPLACE_EXISTING: an existing output remains intact.
            Files.move(staged, output);
        } catch (Exception failure) {
            try {
                Files.deleteIfExists(staged);
            } catch (Exception cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }
}
