package ai.protomolt.proto.asset.bridge;

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

/**
 * A delimiter-separated reader over the rules RFC 4180 states: a field may
 * be quoted with {@code "}, a quoted field may hold the delimiter, newlines,
 * and doubled quotes, and rows end at CR, LF, or CRLF.
 *
 * <p>The delimiter is the producer's, not a guess. A delimited table's claim
 * states it, which is exactly why characterization refuses to identify one:
 * the parsing rules are part of the claim, so a bridge reads them from the
 * format of record.
 */
final class DelimitedReader {

    private static final int EOF = -1;

    private final Reader in;
    private final char delimiter;
    private int pending = Integer.MIN_VALUE;

    DelimitedReader(Reader in, char delimiter) {
        this.in = in;
        this.delimiter = delimiter;
    }

    /**
     * Reads the next row.
     *
     * @return the row's fields, or null at the end of the input
     * @throws IOException when the input fails or a quoted field never closes
     */
    List<String> next() throws IOException {
        int first = read();
        if (first == EOF) {
            return null;
        }
        push(first);
        List<String> fields = new ArrayList<>();
        StringBuilder field = new StringBuilder();
        boolean quoted = false;
        while (true) {
            int c = read();
            if (quoted) {
                if (c == EOF) {
                    throw new IOException("a quoted field never closes");
                }
                if (c == '"') {
                    int after = read();
                    if (after == '"') {
                        field.append('"');
                        continue;
                    }
                    push(after);
                    quoted = false;
                    continue;
                }
                field.append((char) c);
                continue;
            }
            if (c == '"' && field.isEmpty()) {
                quoted = true;
                continue;
            }
            if (c == delimiter) {
                fields.add(field.toString());
                field.setLength(0);
                continue;
            }
            if (c == '\r') {
                int after = read();
                if (after != '\n') {
                    push(after);
                }
                fields.add(field.toString());
                return fields;
            }
            if (c == '\n' || c == EOF) {
                fields.add(field.toString());
                return fields;
            }
            field.append((char) c);
        }
    }

    private int read() throws IOException {
        if (pending != Integer.MIN_VALUE) {
            int held = pending;
            pending = Integer.MIN_VALUE;
            return held;
        }
        return in.read();
    }

    private void push(int c) {
        pending = c;
    }
}
