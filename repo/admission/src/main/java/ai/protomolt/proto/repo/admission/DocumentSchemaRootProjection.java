package ai.protomolt.proto.repo.admission;

import ai.protomolt.proto.repo.v1.DocumentSchemaRootLocator;
import ai.protomolt.proto.repo.v1.RepositoryResolvedSchema;
import ai.protomolt.proto.repo.v1.RepositorySchemaOccurrenceStep;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.validate.source.ProtomoltRuleSource;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CancellationException;

/** Representation of an inventoried root, not proof of publication or retention. */
final class DocumentSchemaRootProjection {
    private static final ProtoValidator VALIDATOR = ProtoValidator.create(List.of(new ProtomoltRuleSource()));
    private DocumentSchemaRootProjection() {}

    static DocumentSchemaRootLocator project(DocumentAnyRootInventory.Result inventory,
            DocumentAnyRootInventory.Root root, Runnable control) {
        Objects.requireNonNull(inventory); Objects.requireNonNull(root); Objects.requireNonNull(control);
        active(control);
        if (!inventory.roots().contains(root)) throw new IllegalArgumentException("root does not belong to inventory");
        var locator = DocumentSchemaRootLocator.newBuilder().setEncodingVersion(1)
                .setSlot(inventory.slot()).setLayoutPolicy(inventory.layoutPolicy())
                .setFragmentSha256(inventory.fragmentSha256()).setFragmentSizeBytes(inventory.original().size())
                .setContainerSchema(RepositoryResolvedSchema.newBuilder()
                        .setSchema(inventory.containerSchema().condition()).setArtifactSha256(inventory.containerSchema().artifactSha256()));
        for (var item : root.access()) {
            active(control);
            var step = RepositorySchemaOccurrenceStep.newBuilder();
            switch (item) {
                case DocumentSchemaOccurrences.Field field -> step.setFieldNumber(field.number());
                case DocumentSchemaOccurrences.Index index -> step.setRepeatedIndex(index.index());
                case DocumentSchemaOccurrences.MapKey key -> step.setMapKey(DocumentSchemaOccurrenceProjection.mapKey(key));
                case DocumentSchemaOccurrences.Boundary ignored -> throw new IllegalArgumentException("root access cannot cross Any");
            }
            locator.addAccess(step);
        }
        var result = locator.build();
        VALIDATOR.validate(result).throwIfInvalid();
        active(control);
        return result;
    }

    private static void active(Runnable control) {
        if (Thread.currentThread().isInterrupted()) throw new CancellationException("root projection interrupted");
        control.run();
    }
}
