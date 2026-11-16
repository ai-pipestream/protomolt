package ai.protomolt.proto.repo.archive;

import ai.protomolt.proto.repo.archive.v1.RenditionDescriptor;
import ai.protomolt.proto.repo.archive.v1.UploadRenditionHeader;
import ai.protomolt.proto.repo.archive.v1.UploadRenditionRequest;
import ai.protomolt.proto.repo.archive.v1.UploadRenditionResponse;
import ai.protomolt.proto.repo.sink.v1.RenditionProvenance;
import ai.protomolt.proto.repo.sink.v1.SinkDestination;
import com.google.protobuf.ByteString;
import com.google.protobuf.Value;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Streams one rendition into {@link RenditionUploads} for {@link RepositorySink}, aborting the
 * stream when the job's lease is lost or cancellation is requested, and renewing the durable lease
 * once more immediately before the final commit.
 */
final class RepositorySinkUpload {
    static final long UPLOAD_READY_WAIT_NANOS = TimeUnit.SECONDS.toNanos(30);

    private final RenditionUploads uploads;
    private final RepositorySinkLeaseCoordinator leases;

    RepositorySinkUpload(
            RenditionUploads uploads,
            RepositorySinkLeaseCoordinator leases) {
        this.uploads = Objects.requireNonNull(uploads, "uploads");
        this.leases = Objects.requireNonNull(leases, "leases");
    }

    UploadRenditionResponse streamUpload(
            RepositorySinkLeaseCoordinator.JobKey jobKey,
            long leaseEpoch,
            RepositorySinkLeaseCoordinator.InFlightJob tracker,
            String commitOperationId,
            String intentSha256,
            SinkDestination dest,
            RenditionDescriptor effectiveRendition,
            boolean parsedDestination,
            ByteString payload,
            String outputSha256,
            RenditionProvenance provisionalProvenance,
            ByteString retainedDescriptors) {
        UploadRenditionHeader.Builder header = UploadRenditionHeader.newBuilder()
                .setOperationId(commitOperationId)
                .setAddress(dest.getAddress())
                .setRendition(effectiveRendition)
                .setExpectedVersion(dest.getExpectedDestinationVersion())
                .setSizeBytes(payload.size())
                .setExpectedSha256(outputSha256);
        if (dest.hasWrittenBy()) {
            header.setWrittenBy(dest.getWrittenBy());
        }
        if (!dest.getFilename().isEmpty()) {
            header.setFilename(dest.getFilename());
        }

        Map<String, Value> blobMetadataFields = parsedDestination
                ? Map.of()
                : RepositorySinkProvenanceCodec.blobMetadataFields(
                        provisionalProvenance, retainedDescriptors);
        Map<String, String> systemSearchMetadata = parsedDestination
                ? RepositorySinkProvenanceCodec.parsedSearchMetadata(
                        provisionalProvenance, retainedDescriptors)
                : Map.of();

        DocumentArchive.SinkUploadCustomization customization =
                new DocumentArchive.SinkUploadCustomization(
                        dest.getCreateOnly(),
                        intentSha256,
                        dest.getTitle(),
                        dest.getContentType(),
                        dest.getSourceUri(),
                        dest.getMetadataMap(),
                        dest.hasSecurity() ? dest.getSecurity() : null,
                        blobMetadataFields,
                        systemSearchMetadata);

        CompletableFuture<UploadRenditionResponse> responseFuture = new CompletableFuture<>();
        RenditionUploads.FlowControlledUpload call =
                uploads.uploadFlowControlled(customization, new StreamObserver<>() {
                    private UploadRenditionResponse value;

                    @Override
                    public void onNext(UploadRenditionResponse response) {
                        this.value = response;
                    }

                    @Override
                    public void onError(Throwable t) {
                        responseFuture.completeExceptionally(t);
                    }

                    @Override
                    public void onCompleted() {
                        if (value == null) {
                            responseFuture.completeExceptionally(
                                    Status.INTERNAL
                                            .withDescription(
                                                    "UploadRendition completed without a receipt")
                                            .asRuntimeException());
                        } else {
                            responseFuture.complete(value);
                        }
                    }
                });

        try {
            call.onNext(UploadRenditionRequest.newBuilder().setHeader(header.build()).build());
            for (int offset = 0;
                    offset < payload.size() && !responseFuture.isDone();
                    offset += RenditionReads.MAX_FRAME_BYTES) {
                abortStreamIfInvalidated(call, tracker);
                awaitUploadReady(call, responseFuture);
                if (responseFuture.isDone()) {
                    break;
                }
                int end = Math.min(payload.size(), offset + RenditionReads.MAX_FRAME_BYTES);
                call.onNext(UploadRenditionRequest.newBuilder()
                        .setChunk(payload.substring(offset, end))
                        .build());
            }
            if (!responseFuture.isDone()) {
                verifyDurableLeaseBeforeFinalCommit(call, jobKey, leaseEpoch, tracker);
                call.onCompleted();
            }
            return responseFuture.get();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            StatusRuntimeException cancelled = Status.CANCELLED
                    .withDescription("Interrupted while streaming rendition upload")
                    .withCause(interrupted)
                    .asRuntimeException();
            call.onError(cancelled);
            throw cancelled;
        } catch (ExecutionException failed) {
            if (failed.getCause() instanceof StatusRuntimeException sre) {
                throw sre;
            }
            throw Status.INTERNAL.withCause(failed.getCause()).asRuntimeException();
        }
    }

    void verifyDurableLeaseBeforeFinalCommit(
            RenditionUploads.FlowControlledUpload call,
            RepositorySinkLeaseCoordinator.JobKey jobKey,
            long leaseEpoch,
            RepositorySinkLeaseCoordinator.InFlightJob tracker) {
        abortStreamIfInvalidated(call, tracker);
        Optional<RepositorySinkJobStore.LeaseRenewal> renewal =
                leases.renewLease(jobKey.accountId(), jobKey.operationId(), leaseEpoch);
        if (renewal.isEmpty()) {
            tracker.markLeaseLost();
            StatusRuntimeException fenced = Status.ABORTED
                    .withDescription("Repository sink job lease lost prior to repository commit")
                    .asRuntimeException();
            call.onError(fenced);
            throw fenced;
        }
        if (renewal.get().cancelRequested()) {
            tracker.cancel(renewal.get().cancelReason());
            StatusRuntimeException cancelled = Status.CANCELLED
                    .withDescription(tracker.reasonOrDefault())
                    .asRuntimeException();
            call.onError(cancelled);
            throw cancelled;
        }
    }

    static void abortStreamIfInvalidated(
            RenditionUploads.FlowControlledUpload call,
            RepositorySinkLeaseCoordinator.InFlightJob tracker) {
        if (tracker.leaseLost.get()) {
            StatusRuntimeException fenced = Status.ABORTED
                    .withDescription("Repository sink job lease lost during upload")
                    .asRuntimeException();
            call.onError(fenced);
            throw fenced;
        }
        if (tracker.cancelled.get()) {
            StatusRuntimeException cancelled = Status.CANCELLED
                    .withDescription(tracker.reasonOrDefault())
                    .asRuntimeException();
            call.onError(cancelled);
            throw cancelled;
        }
        if (tracker.shuttingDown.get()) {
            StatusRuntimeException shutdown = Status.UNAVAILABLE
                    .withDescription("RepositorySink shutting down before commit")
                    .asRuntimeException();
            call.onError(shutdown);
            throw shutdown;
        }
    }

    static void awaitUploadReady(
            RenditionUploads.FlowControlledUpload call,
            CompletableFuture<UploadRenditionResponse> responseFuture)
            throws InterruptedException {
        while (!call.isReady() && !responseFuture.isDone()) {
            if (!call.awaitReady(UPLOAD_READY_WAIT_NANOS) && !responseFuture.isDone()) {
                throw Status.DEADLINE_EXCEEDED
                        .withDescription("Timed out waiting for downstream UploadDocument flow control")
                        .asRuntimeException();
            }
        }
    }
}
