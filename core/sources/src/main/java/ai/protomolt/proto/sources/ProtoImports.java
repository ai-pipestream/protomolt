package ai.protomolt.proto.sources;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Extracts {@code import} statements from {@code .proto} source text without a full parse.
 *
 * <p>This is a syntactic scan used for dependency ordering and reachability; the authoritative
 * check remains compilation ({@link ProtoSourceCompiler}), which fails on imports the scan may
 * have mis-read. Line ({@code //}) and block ({@code /* *&#47;}) comments are stripped before
 * scanning so commented-out imports are ignored; {@code import public} and {@code import weak}
 * are treated as plain imports.</p>
 */
public final class ProtoImports {

    private static final Pattern IMPORT = Pattern.compile(
            "(?m)^\\s*import\\s+(?:public\\s+|weak\\s+)?\"([^\"]+)\"\\s*;");
    private static final Pattern BLOCK_COMMENT = Pattern.compile("/\\*.*?\\*/", Pattern.DOTALL);
    private static final Pattern LINE_COMMENT = Pattern.compile("(?m)//.*$");

    private ProtoImports() {
    }

    /** Returns the import paths declared by the given {@code .proto} source, in order. */
    public static List<String> of(String protoContent) {
        String stripped = LINE_COMMENT.matcher(
                BLOCK_COMMENT.matcher(protoContent).replaceAll(" ")).replaceAll("");
        List<String> imports = new ArrayList<>();
        Matcher matcher = IMPORT.matcher(stripped);
        while (matcher.find()) {
            imports.add(matcher.group(1));
        }
        return List.copyOf(imports);
    }
}
