package ai.protomolt.proto.screening;

import opennlp.tools.namefind.NameFinderME;
import opennlp.tools.namefind.TokenNameFinder;
import opennlp.tools.namefind.TokenNameFinderModel;
import opennlp.tools.tokenize.WhitespaceTokenizer;
import opennlp.tools.util.Span;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * The OpenNLP screening engine: whitespace tokenization, a
 * {@link TokenNameFinder} over the tokens, and token spans mapped back to
 * character offsets. This module is where OpenNLP enters the dependency
 * graph and nowhere else — {@code core/formats} and the validation core stay
 * zero-dependency (the screening chapter's boundary).
 *
 * <p>Model artifacts are data, not code: a production engine loads an
 * operator-mounted {@link TokenNameFinderModel} via {@link #load}; nothing
 * is ever bundled in-tree. The seam constructor exists so hosts and tests
 * can supply any {@link TokenNameFinder} (a dictionary finder, a fake)
 * without a model file.
 *
 * <p>Not thread-safe: {@link NameFinderME} keeps per-document adaptive data.
 * Hosts hold one engine per worker or synchronize.
 */
public final class OpenNlpScreeningEngine implements ScreeningEngine {

    private final TokenNameFinder finder;
    private final String modelVersion;

    /**
     * Wraps any token name finder under a caller-stated model version.
     *
     * @param finder the finder producing token-index spans
     * @param modelVersion the version reported as evidence with every verdict
     */
    public OpenNlpScreeningEngine(TokenNameFinder finder, String modelVersion) {
        this.finder = Objects.requireNonNull(finder, "finder");
        this.modelVersion = Objects.requireNonNull(modelVersion, "modelVersion");
        if (modelVersion.isBlank()) {
            throw new IllegalArgumentException(
                    "modelVersion must be present: a verdict without the model that"
                            + " produced it is not auditable");
        }
    }

    /**
     * Loads a mounted token-name-finder model. The model's own manifest
     * version becomes the evidence version.
     *
     * @param model the serialized {@link TokenNameFinderModel}
     * @return an engine over the loaded model
     */
    public static OpenNlpScreeningEngine load(InputStream model) {
        try {
            TokenNameFinderModel loaded = new TokenNameFinderModel(model);
            return new OpenNlpScreeningEngine(
                    new NameFinderME(loaded), loaded.getVersion().toString());
        } catch (IOException e) {
            throw new UncheckedIOException("failed to load the screening model", e);
        }
    }

    @Override
    public List<Detection> detect(String text) {
        Objects.requireNonNull(text, "text");
        if (text.isBlank()) {
            return List.of();
        }
        Span[] tokens = WhitespaceTokenizer.INSTANCE.tokenizePos(text);
        String[] words = new String[tokens.length];
        for (int i = 0; i < tokens.length; i++) {
            words[i] = tokens[i].getCoveredText(text).toString();
        }
        List<Detection> detections = new ArrayList<>();
        for (Span name : finder.find(words)) {
            int begin = tokens[name.getStart()].getStart();
            int end = tokens[name.getEnd() - 1].getEnd();
            detections.add(new Detection(name.getType(), begin, end, name.getProb()));
        }
        return detections;
    }

    @Override
    public String modelVersion() {
        return modelVersion;
    }
}
