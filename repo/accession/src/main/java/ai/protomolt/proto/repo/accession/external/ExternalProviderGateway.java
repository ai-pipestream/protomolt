package ai.protomolt.proto.repo.accession.external;

import ai.protomolt.proto.grpc.profile.v1.ServiceEndpoint;
import ai.protomolt.proto.grpc.profile.v1.ServiceProfile;
import ai.protomolt.proto.repo.external.v1.ExternalProviderEvidence;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationIntent;
import ai.protomolt.proto.repo.external.v1.ExternalSystemBinding;
import io.grpc.Metadata;
import java.util.Objects;

/**
 * Typed outbound SPI for executing external-system publication/deletion attempts
 * and reconciling unresolved status against external providers.
 *
 * <p>Implementations receive live resolved {@link OutboundAuthContext} headers for
 * outbound transport calls only and must never echo raw credentials into receipts
 * or error diagnostics.
 */
public interface ExternalProviderGateway {

    /**
     * Live outbound transport and credential resolution context resolved from
     * {@code ServiceProfileRepository} and {@code ProfileCredentialResolver}.
     *
     * @param binding the active or historical external-system binding snapshot
     * @param serviceProfile the validated live service profile
     * @param serviceEndpoint the selected live service endpoint
     * @param resolvedHeaders ephemeral outbound headers (never persisted)
     * @param credentialBindingSha256 non-secret SHA-256 over credential reference identifiers
     */
    record OutboundAuthContext(
            ExternalSystemBinding binding,
            ServiceProfile serviceProfile,
            ServiceEndpoint serviceEndpoint,
            Metadata resolvedHeaders,
            String credentialBindingSha256) {

        /** Validates non-null outbound authorization components. */
        public OutboundAuthContext {
            Objects.requireNonNull(binding, "binding");
            Objects.requireNonNull(serviceProfile, "serviceProfile");
            Objects.requireNonNull(serviceEndpoint, "serviceEndpoint");
            Objects.requireNonNull(resolvedHeaders, "resolvedHeaders");
            Objects.requireNonNull(credentialBindingSha256, "credentialBindingSha256");
        }
    }

    /**
     * Outbound publication or deletion invocation bound to a durable operation key.
     *
     * @param operationId stable operation identifier
     * @param intentSha256 lowercase hex SHA-256 of the canonical {@code intent}
     * @param intent semantic publication or deletion intent
     * @param auth live resolved endpoint and credential context
     */
    record ProviderCallRequest(
            String operationId,
            String intentSha256,
            ExternalPublicationIntent intent,
            OutboundAuthContext auth) {

        /** Validates non-null call request components. */
        public ProviderCallRequest {
            Objects.requireNonNull(operationId, "operationId");
            Objects.requireNonNull(intentSha256, "intentSha256");
            Objects.requireNonNull(intent, "intent");
            Objects.requireNonNull(auth, "auth");
        }
    }

    /**
     * Read-only status reconciliation query for an existing operation.
     *
     * @param operationId stable operation identifier being reconciled
     * @param intentSha256 lowercase hex SHA-256 of the canonical {@code intent}
     * @param intent semantic publication or deletion intent
     * @param auth live resolved endpoint and credential context
     * @param includeServingObservation whether advisory serving-layer visibility is requested
     */
    record ProviderStatusQuery(
            String operationId,
            String intentSha256,
            ExternalPublicationIntent intent,
            OutboundAuthContext auth,
            boolean includeServingObservation) {

        /** Validates non-null status query components. */
        public ProviderStatusQuery {
            Objects.requireNonNull(operationId, "operationId");
            Objects.requireNonNull(intentSha256, "intentSha256");
            Objects.requireNonNull(intent, "intent");
            Objects.requireNonNull(auth, "auth");
        }
    }

    /** Outcome of one outbound {@link #execute(ProviderCallRequest)} attempt. */
    sealed interface ProviderCallResult
            permits ProviderCallResult.Receipt, ProviderCallResult.Unresolved {

        /**
         * Structured provider receipt subject to {@link ExternalProviderVerifier} validation.
         *
         * @param evidence candidate provider receipt
         */
        record Receipt(ExternalProviderEvidence evidence) implements ProviderCallResult {
            /** Validates non-null evidence. */
            public Receipt {
                Objects.requireNonNull(evidence, "evidence");
            }
        }

        /**
         * Lost reply, deadline expiry, or endpoint unavailability leaving external
         * state unresolved until reconciled. Never treated as rollback or confirmation.
         *
         * @param diagnosticCode bounded machine diagnostic code
         * @param diagnosticMessage sanitized non-secret diagnostic summary
         */
        record Unresolved(String diagnosticCode, String diagnosticMessage)
                implements ProviderCallResult {
            /** Validates non-null diagnostic code and message. */
            public Unresolved {
                Objects.requireNonNull(diagnosticCode, "diagnosticCode");
                Objects.requireNonNull(diagnosticMessage, "diagnosticMessage");
            }
        }
    }

    /** Outcome of a read-only {@link #queryStatus(ProviderStatusQuery)} lookup. */
    sealed interface ProviderStatusResult
            permits ProviderStatusResult.Receipt,
                    ProviderStatusResult.NoRecordedOutcome,
                    ProviderStatusResult.Unavailable {

        /**
         * Provider returned durable operation/journal evidence subject to verification.
         *
         * @param evidence candidate provider receipt
         */
        record Receipt(ExternalProviderEvidence evidence) implements ProviderStatusResult {
            /** Validates non-null evidence. */
            public Receipt {
                Objects.requireNonNull(evidence, "evidence");
            }
        }

        /**
         * Provider lookup succeeded and found no recorded outcome for the operation key.
         * Does not prove an earlier in-flight network write stopped; retry remains valid.
         *
         * @param diagnosticMessage sanitized status detail
         */
        record NoRecordedOutcome(String diagnosticMessage) implements ProviderStatusResult {
            /** Validates non-null diagnostic message. */
            public NoRecordedOutcome {
                Objects.requireNonNull(diagnosticMessage, "diagnosticMessage");
            }
        }

        /**
         * Provider status endpoint is currently unreachable or timed out.
         *
         * @param diagnosticCode bounded machine diagnostic code
         * @param diagnosticMessage sanitized diagnostic summary
         */
        record Unavailable(String diagnosticCode, String diagnosticMessage)
                implements ProviderStatusResult {
            /** Validates non-null diagnostic code and message. */
            public Unavailable {
                Objects.requireNonNull(diagnosticCode, "diagnosticCode");
                Objects.requireNonNull(diagnosticMessage, "diagnosticMessage");
            }
        }
    }

    /**
     * Executes one bounded publication or deletion attempt against the external provider.
     *
     * @param request typed call request
     * @return structured receipt or unresolved outcome
     */
    ProviderCallResult execute(ProviderCallRequest request);

    /**
     * Queries the external provider's durable status for an existing operation without
     * performing a new mutating write.
     *
     * @param query typed status query
     * @return provider receipt, absence of recorded outcome, or endpoint unavailability
     */
    ProviderStatusResult queryStatus(ProviderStatusQuery query);
}
