package ai.protomolt.proto.inference.spi;

/**
 * The model id (or provider id) does not exist in the catalog. Carved out of
 * {@link InferenceException} so transport surfaces can answer NOT_FOUND
 * instead of a generic internal error.
 */
public final class UnknownModelException extends InferenceException {

    private static final long serialVersionUID = 1L;

    /**
     * Creates the exception naming the unknown id.
     *
     * @param message the unknown id and where it was looked up
     */
    public UnknownModelException(String message) {
        super(message);
    }
}
