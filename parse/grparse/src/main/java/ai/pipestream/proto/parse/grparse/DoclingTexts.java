package ai.pipestream.proto.parse.grparse;

import ai.pipestream.document.v1.BaseTextItem;
import ai.pipestream.document.v1.Document;
import ai.pipestream.document.v1.TextItemBase;
import ai.pipestream.parse.v1.PageData;

/**
 * Text extraction over the fleet document model: how a page's or document's
 * heterogeneous {@link BaseTextItem}s reduce to plain text. Shared by the
 * page stream ({@code ParsedPage.text}) and the claims fold (the title
 * claim).
 */
final class DoclingTexts {

    private DoclingTexts() {
    }

    /** Concatenates a page's text items' text in list order, newline-separated. */
    static String pageText(PageData page) {
        StringBuilder text = new StringBuilder();
        for (BaseTextItem item : page.getTextsList()) {
            String itemText = textOf(item);
            if (!itemText.isBlank()) {
                if (!text.isEmpty()) {
                    text.append('\n');
                }
                text.append(itemText);
            }
        }
        return text.toString();
    }

    /** The first non-blank title item's text; blank when none exists. */
    static String titleOf(Document document) {
        for (BaseTextItem item : document.getTextsList()) {
            if (item.getItemCase() == BaseTextItem.ItemCase.TITLE) {
                String text = item.getTitle().getBase().getText();
                if (!text.isBlank()) {
                    return text;
                }
            }
        }
        return "";
    }

    static String textOf(BaseTextItem item) {
        // CodeItem inlines its fields instead of embedding TextItemBase.
        if (item.getItemCase() == BaseTextItem.ItemCase.CODE) {
            return item.getCode().getText();
        }
        TextItemBase base =
                switch (item.getItemCase()) {
                    case TITLE -> item.getTitle().getBase();
                    case SECTION_HEADER -> item.getSectionHeader().getBase();
                    case LIST_ITEM -> item.getListItem().getBase();
                    case FORMULA -> item.getFormula().getBase();
                    case TEXT -> item.getText().getBase();
                    case FIELD_HEADING -> item.getFieldHeading().getBase();
                    case FIELD_VALUE -> item.getFieldValue().getBase();
                    case CODE, ITEM_NOT_SET -> null;
                };
        return base == null ? "" : base.getText();
    }
}
