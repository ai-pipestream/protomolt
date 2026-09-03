package ai.protomolt.proto.parse.grparse;

import ai.protomolt.proto.parse.document.v1.ImageRef;
import ai.protomolt.proto.parse.grparse.v1.PageData;
import ai.protomolt.proto.parse.plugin.v1.PagePreview;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Preview forwarding: a page's fleet-rendered image (embedded in
 * {@code PageData.page_meta.image} as a data URI) decoded into the plugin
 * contract's {@link PagePreview}.
 */
final class PagePreviews {

    private static final Logger LOG = LoggerFactory.getLogger(PagePreviews.class);

    private PagePreviews() {
    }

    /**
     * The preview for one page, or {@code null} when there is nothing to
     * forward. Pages without images are simply pages the fleet did not
     * render; non-data URIs reference storage this adapter has no business
     * fetching. Both are skipped, never failed.
     *
     * @param page the page whose metadata may carry a rendered image
     * @return the decoded preview, or {@code null} to skip
     */
    static PagePreview fromPage(PageData page) {
        if (!page.getPageMeta().hasImage()) {
            return null;
        }
        ImageRef image = page.getPageMeta().getImage();
        DataUri decoded = DataUri.parse(image.getUri());
        if (decoded == null) {
            LOG.debug(
                    "page {} image is not a base64 data URI; no preview forwarded",
                    page.getPageNumber());
            return null;
        }
        return PagePreview.newBuilder()
                .setPageNumber(page.getPageNumber())
                .setMimeType(decoded.mimeType())
                .setImage(decoded.data())
                .setWidth((int) Math.round(image.getSize().getWidth()))
                .setHeight((int) Math.round(image.getSize().getHeight()))
                .build();
    }
}
