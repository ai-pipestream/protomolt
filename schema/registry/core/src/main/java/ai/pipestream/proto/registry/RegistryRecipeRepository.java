package ai.pipestream.proto.registry;

import ai.pipestream.proto.grpc.recipe.RecipeRepository;
import ai.pipestream.proto.grpc.recipe.v1.VersionedRecipe;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * The recipe promotion contract backed by the registry's git repository: every promoted
 * version is one committed, immutable {@code recipes/<name>/<version>.pb} alongside the
 * schema subjects, so a recipe's provenance lives in the same reviewable history as the
 * contracts it was checked against.
 *
 * <p>All storage semantics (validation, immutability, idempotent re-promotion, locking) live
 * in {@link GitSchemaRegistryStore}; this adapter only maps the {@link RecipeRepository}
 * vocabulary onto them.</p>
 */
public final class RegistryRecipeRepository implements RecipeRepository {

    private final GitSchemaRegistryStore store;

    /** An adapter over the given store; the caller keeps ownership of the store's lifecycle. */
    public RegistryRecipeRepository(GitSchemaRegistryStore store) {
        this.store = Objects.requireNonNull(store, "store");
    }

    @Override
    public Optional<VersionedRecipe> find(String name, String version) {
        return store.recipe(name, version);
    }

    @Override
    public List<VersionedRecipe> versions(String name) {
        return store.recipeVersions(name);
    }

    @Override
    public void save(VersionedRecipe recipe) {
        store.putRecipe(recipe);
    }
}
