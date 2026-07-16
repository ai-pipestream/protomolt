package ai.pipestream.proto.lake.iceberg.s3;

import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.HttpClientProperties;
import org.apache.iceberg.aws.s3.S3FileIOProperties;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Catalog properties that put an Iceberg table's data on an S3-compatible object store through
 * Iceberg's own {@code S3FileIO} - the escape hatch from a local filesystem to RustFS, SeaweedFS,
 * Ceph, or AWS S3, with no Hadoop and no MinIO.
 *
 * <p>Merge the returned map into the properties passed to {@code Catalog.initialize}. The store is
 * the operator's choice, configured here, never a request argument - the same disk-footprint
 * stance the rest of the toolkit takes. {@code S3FileIO} is only the file plane; the catalog still
 * owns atomic commit, so no store needs S3 conditional writes.</p>
 */
public final class S3Catalogs {

    private S3Catalogs() {
    }

    /**
     * {@code S3FileIO} properties for a self-hosted, path-style S3 store reached at
     * {@code endpoint} (e.g. {@code http://localhost:9000} for RustFS or SeaweedFS). For real AWS
     * S3, prefer {@link #awsRegion} instead, which lets the SDK resolve the region's endpoint and
     * pick up ambient credentials.
     */
    public static Map<String, String> pathStyle(String endpoint, String region,
                                                 String accessKeyId, String secretAccessKey) {
        Map<String, String> props = fileIo(region);
        props.put(S3FileIOProperties.ENDPOINT, endpoint);
        props.put(S3FileIOProperties.PATH_STYLE_ACCESS, "true");
        props.put(S3FileIOProperties.ACCESS_KEY_ID, accessKeyId);
        props.put(S3FileIOProperties.SECRET_ACCESS_KEY, secretAccessKey);
        return props;
    }

    /**
     * {@code S3FileIO} properties for real AWS S3 in {@code region}: no endpoint override and no
     * static credentials, so the AWS SDK's default provider chain (env, profile, IMDS, IRSA)
     * supplies them.
     */
    public static Map<String, String> awsRegion(String region) {
        return fileIo(region);
    }

    private static Map<String, String> fileIo(String region) {
        Map<String, String> props = new LinkedHashMap<>();
        props.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO");
        props.put(AwsClientProperties.CLIENT_REGION, region);
        // iceberg-aws defaults to the Apache HTTP client; use the JDK one we actually ship.
        props.put(HttpClientProperties.CLIENT_TYPE, HttpClientProperties.CLIENT_TYPE_URLCONNECTION);
        return props;
    }
}
