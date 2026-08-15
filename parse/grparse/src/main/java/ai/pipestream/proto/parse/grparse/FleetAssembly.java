package ai.pipestream.proto.parse.grparse;

import ai.pipestream.document.v1.Document;
import ai.pipestream.parse.v1.CollectorDocument;
import ai.pipestream.parse.v1.CollectorFailure;
import ai.pipestream.parse.v1.DocumentComplete;
import ai.pipestream.parse.v1.PageData;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;

/**
 * Final assembly: how one parse's accumulated gRParse events reduce to the
 * single fleet-model document and the degradation warnings the coordinator
 * stores as PARTIAL.
 */
final class FleetAssembly {

    private FleetAssembly() {
    }

    /**
     * The final fleet-model document. A single collector document with no
     * page stream IS the output; otherwise pages concatenate in page order
     * and any collector documents append additively after them (gRParse's
     * own scatter-gather merge model).
     *
     * @param pages the parse's pages by page number, in page order
     * @param collectorDocuments documents from out-of-process collectors
     * @param documentComplete the terminal event, or {@code null} when the
     *        stream ended without one
     * @param filename the requested filename, used as the document name
     * @return the assembled document
     */
    static Document assemble(
            SortedMap<Integer, PageData> pages,
            List<CollectorDocument> collectorDocuments,
            DocumentComplete documentComplete,
            String filename) {
        if (pages.isEmpty() && collectorDocuments.size() == 1) {
            return collectorDocuments.getFirst().getDocument();
        }
        Document.Builder document = Document.newBuilder()
                .setSchemaName("docling_document_v2")
                .setName(filename);
        if (documentComplete != null && documentComplete.hasOrigin()) {
            document.setOrigin(documentComplete.getOrigin());
        }
        for (PageData page : pages.values()) {
            document.addAllTexts(page.getTextsList());
            document.addAllTables(page.getTablesList());
            document.addAllPictures(page.getPicturesList());
            if (page.hasPageMeta()) {
                document.putPages(page.getPageNumber(), page.getPageMeta());
            }
        }
        for (CollectorDocument collected : collectorDocuments) {
            document.addAllTexts(collected.getDocument().getTextsList());
            document.addAllTables(collected.getDocument().getTablesList());
            document.addAllPictures(collected.getDocument().getPicturesList());
        }
        return document.build();
    }

    /**
     * One warning line per degradation gRParse reported: every
     * {@code CollectorFailure}, plus each collector document's own
     * extraction warnings. Non-empty warnings make the stored result
     * PARTIAL — losses are reported, never dropped.
     *
     * @param documentComplete the terminal event, or {@code null} when the
     *        stream ended without one
     * @param collectorDocuments documents from out-of-process collectors
     * @return the warning lines, possibly empty
     */
    static List<String> warningsOf(
            DocumentComplete documentComplete, List<CollectorDocument> collectorDocuments) {
        List<String> warnings = new ArrayList<>();
        if (documentComplete != null) {
            for (CollectorFailure failure : documentComplete.getCollectorFailuresList()) {
                warnings.add(
                        "collector " + failure.getCollector().name() + ": " + failure.getError());
            }
        }
        for (CollectorDocument collected : collectorDocuments) {
            for (String warning : collected.getWarningsList()) {
                warnings.add("collector " + collected.getCollector().name() + ": " + warning);
            }
        }
        return warnings;
    }
}
