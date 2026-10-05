package ai.protomolt.proto.repo.admission;

import ai.protomolt.proto.validate.RuleCompilationException;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.junit.jupiter.api.Test;
import java.time.Instant;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import static ai.protomolt.proto.repo.admission.DocumentPayloadCheckTest.*;
import static org.assertj.core.api.Assertions.*;

class DocumentPayloadAssessmentTest {
    private static final String URL = "type.protomolt.test/payload.Choice";
    private static final Instant AT = Instant.parse("2000-01-01T00:00:00Z");
    private static final DocumentPayloadCheck.Limits LIMITS = new DocumentPayloadCheck.Limits(4096, 100, 10, 100, 1000);

    @Test void invalidValueStillResolvesLaterVersionsAndKeepsCompleteOccurrences() throws Exception {
        var outer = wrapper();
        var first = binding(choice("this.left != '' && this.right == ''"));
        var second = binding(choice("this.right != '' && this.left == ''"));
        var payload = wrapped(outer, candidate(first, "both", "filled"), candidate(second, "", "right"));
        var calls = new AtomicInteger();
        var result = assess(asset(outer, payload.getTypeUrl()), payload,
                request -> asset(calls.getAndIncrement() == 0 ? first : second, URL));
        assertThat(result).isInstanceOf(DocumentPayloadAssessment.Invalid.class);
        var invalid = (DocumentPayloadAssessment.Invalid) result;
        assertThat(calls.get()).isEqualTo(2);
        assertThat(invalid.assets()).hasSize(3);
        assertThat(invalid.occurrences()).hasSize(3);
        assertThat(invalid.original()).isSameAs(payload);
        assertThat(invalid.evaluatedAt()).isEqualTo(AT);
        assertThat(invalid.failure().ruleId()).isEqualTo("choice.exclusive");
        assertThat(invalid.failure().occurrence()).contains(new DocumentSchemaOccurrences.Index(0));
        assertThatThrownBy(() -> invalid.assets().clear()).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test void missingLaterSchemaPreventsAnInvalidAssessment() throws Exception {
        var outer = wrapper();
        var inner = binding(choice("false"));
        var payload = wrapped(outer, candidate(inner, "left", ""), candidate(inner, "right", ""));
        var calls = new AtomicInteger();
        assertThatThrownBy(() -> assess(asset(outer, payload.getTypeUrl()), payload,
                request -> calls.getAndIncrement() == 0 ? asset(inner, URL) : null))
                .hasMessageContaining("unresolved Any schema asset");
        assertThat(calls.get()).isEqualTo(2);
    }

    @Test void unsupportedLaterRuleAndMalformedLaterBytesRemainFailures() throws Exception {
        var outer = wrapper();
        var inner = binding(choice("false"));
        var unsupported = binding(choice("this.no_such_field == 1"));
        var payload = wrapped(outer, candidate(inner, "left", ""), candidate(unsupported, "right", ""));
        var calls = new AtomicInteger();
        assertThatThrownBy(() -> assess(asset(outer, payload.getTypeUrl()), payload,
                request -> asset(calls.getAndIncrement() == 0 ? inner : unsupported, URL)))
                .isInstanceOf(RuleCompilationException.class);
        var malformed = wrapped(outer, candidate(inner, "left", ""),
                Any.newBuilder().setTypeUrl(URL).setValue(ByteString.copyFrom(new byte[]{10, 4, 1})).build());
        assertThatThrownBy(() -> assess(asset(outer, malformed.getTypeUrl()), malformed, request -> asset(inner, URL)))
                .isInstanceOf(InvalidProtocolBufferException.class);
    }

    @Test void resolverCancellationAndOccurrenceLimitsAreNotInvalidVerdicts() throws Exception {
        var outer = wrapper();
        var inner = binding(choice("false"));
        var payload = wrapped(outer, candidate(inner, "left", ""), candidate(inner, "right", ""));
        var failure = new CancellationException("cancelled resolution");
        var calls = new AtomicInteger();
        assertThatThrownBy(() -> assess(asset(outer, payload.getTypeUrl()), payload, request -> {
            if (calls.getAndIncrement() == 1) throw failure;
            return asset(inner, URL);
        })).isSameAs(failure);
        assertThatThrownBy(() -> DocumentPayloadCheck.assessContextualAssets(asset(outer, payload.getTypeUrl()), payload,
                payload.getTypeUrl(), validator(), LIMITS, () -> {}, request -> asset(inner, URL),
                new DocumentSchemaOccurrences.Limits(2, 100, 10000), AT))
                .hasMessageContaining("occurrence count limit");
    }

    @Test void acceptedAssessmentUsesPinnedTimeAndStillReturnsCheckedPayload() throws Exception {
        var inner = binding(choice("now == timestamp('2000-01-01T00:00:00Z')"));
        var payload = candidate(inner, "left", "");
        var root = asset(inner, URL);
        var accepted = assess(root, payload, request -> { throw new AssertionError("No nested Any"); });
        assertThat(accepted).isInstanceOf(DocumentPayloadAssessment.Accepted.class);
        assertThat(((DocumentPayloadAssessment.Accepted) accepted).checked().payload().original()).isSameAs(payload);
        assertThat(DocumentPayloadCheck.assessContextualAssets(root, payload, URL, validator(), LIMITS, () -> {},
                request -> { throw new AssertionError("No nested Any"); }, DocumentSchemaOccurrences.Limits.DEFAULT, AT.plusSeconds(1)))
                .isInstanceOf(DocumentPayloadAssessment.Invalid.class);
    }

    @Test void strictCheckPinsTimeAcrossNestedAnyValues() throws Exception {
        var outer = wrapper();
        var inner = binding(choice("now == timestamp('2000-01-01T00:00:00Z')"));
        var payload = wrapped(outer, candidate(inner, "left", ""), candidate(inner, "right", ""));
        var calls = new AtomicInteger();
        var checked = DocumentPayloadCheck.checkContextualAssets(asset(outer, payload.getTypeUrl()), payload,
                payload.getTypeUrl(), validator(), LIMITS, () -> {}, request -> {
                    calls.incrementAndGet();
                    return asset(inner, URL);
                }, DocumentSchemaOccurrences.Limits.DEFAULT, AT);
        assertThat(checked.payload().occurrences()).hasSize(3);
        assertThat(calls.get()).isEqualTo(2);
        assertThatThrownBy(() -> DocumentPayloadCheck.checkContextualAssets(asset(outer, payload.getTypeUrl()), payload,
                payload.getTypeUrl(), validator(), LIMITS, () -> {}, request -> asset(inner, URL),
                DocumentSchemaOccurrences.Limits.DEFAULT, AT.plusSeconds(1)))
                .isInstanceOf(ai.protomolt.proto.validate.ValidationResult.ValidationException.class);
    }

    private static DocumentPayloadAssessment assess(DocumentSchemaAssetBinding root, Any payload,
            Function<DocumentPayloadCheck.ResolutionRequest, DocumentSchemaAssetBinding> resolver) throws Exception {
        return DocumentPayloadCheck.assessContextualAssets(root, payload, payload.getTypeUrl(), validator(), LIMITS,
                () -> {}, resolver, DocumentSchemaOccurrences.Limits.DEFAULT, AT);
    }
}
