package ai.protomolt.proto.repo.catalog;

import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;

/**
 * A catalog operation failed. {@link #unavailable()} distinguishes an unreachable
 * or overloaded catalog, which is never a recorded outcome, from a failure that
 * would recur on retry.
 */
public class CatalogException extends RuntimeException {
    private static final String UNIQUE_VIOLATION_SQL_STATE = "23505";

    private final boolean unavailable;

    public CatalogException(String message, Throwable cause, boolean unavailable) {
        super(message, cause);
        this.unavailable = unavailable;
    }

    public boolean unavailable() { return unavailable; }

    /**
     * Returns whether this failure is a unique-constraint conflict ({@code SQLState 23505}).
     *
     * @return {@code true} when the underlying SQL failure is a unique-constraint violation
     */
    public boolean alreadyExists() {
        return !unavailable
                && getCause() instanceof SQLException sql
                && UNIQUE_VIOLATION_SQL_STATE.equals(sql.getSQLState());
    }

    static CatalogException from(String operation, SQLException failure) {
        return new CatalogException(operation + " failed: " + failure.getMessage(), failure, unavailable(failure));
    }

    /** Connection, resource and shutdown classes, and pool acquisition timeouts. */
    static boolean unavailable(SQLException failure) {
        if (failure instanceof SQLTransientConnectionException) return true;
        var state = failure.getSQLState();
        return state != null && (state.startsWith("08") || state.startsWith("53") || state.startsWith("57P"));
    }
}
