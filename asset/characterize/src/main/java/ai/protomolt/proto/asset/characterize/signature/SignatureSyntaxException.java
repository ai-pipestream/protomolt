package ai.protomolt.proto.asset.characterize.signature;

/**
 * A signature expression the compiler will not accept, with the position
 * in the expression where it gave up.
 *
 * <p>Signature definitions are data, and data can be wrong. A definition
 * that fails to compile is skipped by name and the rest of the set still
 * loads, so one bad expression costs one format's identification instead
 * of all of them.
 */
public class SignatureSyntaxException extends Exception {

    private static final long serialVersionUID = 1L;

    private final String expression;
    private final int position;

    /**
     * Creates the exception.
     *
     * @param expression the expression that would not compile
     * @param position the character index where compilation gave up
     * @param detail what was wrong
     */
    public SignatureSyntaxException(String expression, int position, String detail) {
        super(detail + " at position " + position + " of \"" + expression + "\"");
        this.expression = expression;
        this.position = position;
    }

    /** The expression that would not compile. */
    public String expression() {
        return expression;
    }

    /** The character index where compilation gave up. */
    public int position() {
        return position;
    }
}
