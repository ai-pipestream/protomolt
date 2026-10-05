package ai.protomolt.proto.repo.spi;

import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.DocumentManifest;
import ai.protomolt.proto.repo.v1.NodeAddress;
import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/** Exact immutable revision reads, separate from the current-document/partial assembly API. */
public interface HistoricalDocumentRepository {
    /** Original fragments, without deserialization or a schema-validity claim. */
    RawRead readRaw(RepositoryCaller caller, NodeAddress address, UUID revision, RepositoryReadControl control);

    /** Retained-contract validation with the current runtime; never falls back to raw delivery. */
    ValidatedRead readValidated(RepositoryCaller caller, NodeAddress address, UUID revision, RepositoryReadControl control);

    /** Metadata and content are borrowed until close; keep open through actual consumption/serialization. */
    interface RevisionRead extends AutoCloseable {
        NodeAddress address();
        UUID revision();
        long publicationRevision();
        DocumentManifest manifest();
        /** Recheck the bound caller's current READ access immediately before delivery. Requires an open result. */
        void authorizeDelivery(RepositoryReadControl control);
        /** Local ownership release; hosts separately reconcile drained SQL pins. */
        @Override void close();
    }

    /** Read-only view of one present fragment at its ordinal in the complete captured manifest. */
    record Fragment(int revisionOrdinal, ByteBuffer bytes) {
        public Fragment {
            if (revisionOrdinal < 0) throw new IllegalArgumentException("Negative fragment ordinal");
            bytes = Objects.requireNonNull(bytes).asReadOnlyBuffer();
        }
        /** Each caller gets its own cursor; the backing payload remains borrowed. */
        @Override public ByteBuffer bytes() { return bytes.asReadOnlyBuffer(); }
    }

    interface RawRead extends RevisionRead {
        List<Fragment> fragments();
    }

    interface ValidatedRead extends RevisionRead {
        Document document();
        String validationProfile();
        String policySha256();
        ByteString commandSha256();
    }
}
