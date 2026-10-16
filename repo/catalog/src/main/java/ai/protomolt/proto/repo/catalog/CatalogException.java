package ai.protomolt.proto.repo.catalog;

import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;

/**
 * A catalog operation failed. {@link #unavailable()} distinguishes an unreachable
 * or overloaded catalog, which is never a recorded outcome, from a failure that
 * would recur on retry.
 */
public class CatalogException extends RuntimeException {
    private final boolean unavailable;

    public CatalogException(String message, Throwable cause, boolean unavailable) {
        super(message, cause);
        this.unavailable = unavailable;
    }

    public boolean unavailable() { return unavailable; }

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
