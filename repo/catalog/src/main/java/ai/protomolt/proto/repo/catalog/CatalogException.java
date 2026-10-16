package ai.protomolt.proto.repo.catalog;

import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;

/**
 * A catalog operation failed. {@link #unavailable()} distinguishes an unreachable
 * or overloaded catalog, which is never a recorded outcome, from a failure that
 * would recur on retry.
 */
public class CatalogException extends RuntimeException {
    private static final String SQLSTATE_CONNECTION_EXCEPTION_PREFIX = "08";
    private static final String SQLSTATE_INSUFFICIENT_RESOURCES_PREFIX = "53";
    private static final String SQLSTATE_OPERATOR_INTERVENTION_PREFIX = "57P";

    private final boolean unavailable;

    /**
     * Constructs a catalog exception.
     *
     * @param message the detail message explaining the failure
     * @param cause the underlying cause of the failure
     * @param unavailable whether the error indicates an unavailable or overloaded catalog
     */
    public CatalogException(String message, Throwable cause, boolean unavailable) {
        super(message, cause);
        this.unavailable = unavailable;
    }

    /**
     * Indicates whether this failure is due to an unreachable or overloaded catalog.
     *
     * @return true if the catalog is temporarily unavailable, false otherwise
     */
    public boolean unavailable() { return unavailable; }

    static CatalogException from(String operation, SQLException failure) {
        return new CatalogException(operation + " failed: " + failure.getMessage(), failure, unavailable(failure));
    }

    /**
     * Determines whether the given SQL exception corresponds to an unavailable condition.
     * Connection, resource and shutdown classes, and pool acquisition timeouts.
     *
     * @param failure the SQL exception to inspect
     * @return true if the exception indicates transient connection, resource, or shutdown issues
     */
    static boolean unavailable(SQLException failure) {
        if (failure instanceof SQLTransientConnectionException) return true;
        var state = failure.getSQLState();
        return state != null && (state.startsWith(SQLSTATE_CONNECTION_EXCEPTION_PREFIX)
                || state.startsWith(SQLSTATE_INSUFFICIENT_RESOURCES_PREFIX)
                || state.startsWith(SQLSTATE_OPERATOR_INTERVENTION_PREFIX));
    }
}
