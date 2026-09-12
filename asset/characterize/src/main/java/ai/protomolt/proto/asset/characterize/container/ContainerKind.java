package ai.protomolt.proto.asset.characterize.container;

/**
 * A container structure whose members can be listed and read.
 *
 * <p>Some formats are not a byte layout at all, they are a filing system
 * with documents inside. Two of them account for most of the office
 * documents in existence, and both are opaque to leading-magic detection:
 * every OOXML document, every OpenDocument file, every EPUB and every JAR
 * starts with the same four bytes, and every legacy Office document starts
 * with the same eight. What separates them is which members are inside.
 */
public enum ContainerKind {

    /** The ZIP archive layout, behind OOXML, OpenDocument, EPUB and JAR. */
    ZIP,

    /** The compound file layout, behind legacy Word, Excel, PowerPoint and Outlook. */
    OLE2
}
