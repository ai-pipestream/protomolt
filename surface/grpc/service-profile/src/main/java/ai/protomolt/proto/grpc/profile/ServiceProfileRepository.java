package ai.protomolt.proto.grpc.profile;

import ai.protomolt.proto.grpc.profile.v1.DescriptorArtifact;
import ai.protomolt.proto.grpc.profile.v1.ServiceProfile;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/** Durable access to service profiles and their separately stored descriptor artifacts. */
public interface ServiceProfileRepository {

    /** Loads a profile by its stable logical name. */
    Optional<ServiceProfile> find(String name) throws IOException;

    /** Lists all stored profiles in stable name order. */
    List<ServiceProfile> list() throws IOException;

    /** Atomically creates or replaces a profile. */
    void save(ServiceProfile profile) throws IOException;

    /** Loads a descriptor artifact by its lowercase SHA-256 fingerprint. */
    Optional<DescriptorArtifact> findDescriptorArtifact(String fingerprint) throws IOException;

    /** Atomically stores a descriptor artifact under its content fingerprint. */
    void saveDescriptorArtifact(DescriptorArtifact artifact) throws IOException;
}
