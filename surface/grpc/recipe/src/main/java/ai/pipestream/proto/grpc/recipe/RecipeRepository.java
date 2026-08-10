package ai.pipestream.proto.grpc.recipe;

import ai.pipestream.proto.grpc.recipe.v1.VersionedRecipe;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/** Durable immutable versions of descriptor-grounded gRPC recipes. */
public interface RecipeRepository {

    /** Finds one exact recipe version. */
    Optional<VersionedRecipe> find(String name, String version) throws IOException;

    /** Lists all versions of one recipe in repository-defined stable order. */
    List<VersionedRecipe> versions(String name) throws IOException;

    /** Saves a new immutable version or verifies that an existing version is identical. */
    void save(VersionedRecipe recipe) throws IOException;
}
