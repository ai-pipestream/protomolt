/**
 * Puts an Iceberg table's data files on an S3-compatible object store.
 *
 * <p>{@link S3Catalogs} builds the property maps that select Iceberg's own {@code S3FileIO} and
 * configure it, either for a self-hosted path-style store reached at an explicit endpoint or for
 * AWS S3 in a named region. Merge the result into the properties passed to
 * {@code Catalog.initialize}. No Hadoop runtime is involved, and the store is configured by the
 * operator rather than named in a request.</p>
 *
 * <p>{@code S3FileIO} covers the file plane only; the catalog still owns atomic commit, so the
 * store needs no conditional-write support. Everything above the file plane — table creation,
 * schema conversion, appends — stays in {@link ai.pipestream.proto.lake.iceberg}, whose
 * {@link ai.pipestream.proto.lake.iceberg.LocalFileIO} is the local-filesystem counterpart to
 * this package.</p>
 *
 * <p>See the <a href="https://github.com/ai-pipestream/protomolt/blob/main/docs/iceberg.md">Apache
 * Iceberg guide</a> for the object-store lane.</p>
 */
package ai.pipestream.proto.lake.iceberg.s3;
