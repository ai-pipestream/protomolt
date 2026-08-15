package ai.pipestream.proto.acquire.pull;

import java.util.ArrayList;
import java.util.List;

/**
 * The outcome of one pull pass: counts, per-item errors, and the watermark to hand to the next
 * pull. Connectors are stateless — the watermark is the caller's to keep.
 *
 * @param submitted items newly saved through the door
 * @param deduplicated items the repository recognized as already present
 * @param failed items that errored; they stay behind the watermark and retry next pull
 * @param errors one line per failed item
 * @param watermark the high-water mark for the next pull's input
 */
public record PullReport(int submitted, int deduplicated, int failed,
                         List<String> errors, String watermark) {

    /**
     * Accumulates a pull pass. The watermark only advances through an unbroken prefix of
     * successes: the first failure freezes it, so the failed item (and everything after it)
     * stays ahead of the watermark and is retried by the next pull instead of silently lost.
     */
    public static final class Accumulator {

        private final List<String> errors = new ArrayList<>();
        private int submitted;
        private int deduplicated;
        private int failed;
        private String watermark;
        private boolean frozen;

        /** Starts from the pull's input watermark (blank for a first pull). */
        public Accumulator(String initialWatermark) {
            this.watermark = initialWatermark == null ? "" : initialWatermark;
        }

        /** Records one successful submission and advances the watermark unless frozen. */
        public void success(boolean deduplicated, String mark) {
            if (deduplicated) {
                this.deduplicated++;
            } else {
                this.submitted++;
            }
            if (!frozen) {
                this.watermark = mark;
            }
        }

        /** Records one failed item and freezes the watermark. */
        public void failure(String error) {
            failed++;
            errors.add(error);
            frozen = true;
        }

        /** The finished report. */
        public PullReport report() {
            return new PullReport(submitted, deduplicated, failed, List.copyOf(errors), watermark);
        }
    }
}
