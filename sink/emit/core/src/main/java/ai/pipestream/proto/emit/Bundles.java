package ai.pipestream.proto.emit;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/** Bundle helpers that need no destination at all. */
public final class Bundles {

    private Bundles() {
    }

    /**
     * The bundle as one zip archive, built entirely in memory — the form a verb response
     * carries a whole bundle in without the server touching disk. Entries keep bundle order
     * and carry a fixed timestamp, so the same bundle always zips to the same bytes.
     */
    public static byte[] zip(Bundle bundle) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (ZipOutputStream zip = new ZipOutputStream(out)) {
            for (String path : bundle.paths()) {
                ZipEntry entry = new ZipEntry(path);
                entry.setTime(0L);
                zip.putNextEntry(entry);
                zip.write(bundle.file(path));
                zip.closeEntry();
            }
        } catch (IOException e) {
            throw new UncheckedIOException("In-memory zip cannot fail on IO", e);
        }
        return out.toByteArray();
    }
}
