package ai.protomolt.proto.account.service.store;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.account.v1.AccountStatus;
import java.lang.reflect.Proxy;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * Unit tests verifying that {@link JdbcAccountStore#listWithin} frees the JDBC
 * {@link Array} without swallowing cleanup failures or masking primary query
 * failures.
 */
class JdbcAccountStoreArrayCleanupTest {

    @Test
    void listWithinPropagatesArrayFreeFailureWhenQueriesSucceed() {
        SQLException freeFailure = new SQLException("ids array free failed");
        Array idsArray = proxyArray(freeFailure);
        Connection connection = proxyConnection(idsArray, null);
        JdbcAccountStore store = new JdbcAccountStore();

        assertThatThrownBy(() -> store.listWithin(
                connection,
                List.of("acct-1", "acct-2"),
                AccountStatus.ACCOUNT_STATUS_UNSPECIFIED,
                10,
                0L))
                .isInstanceOfSatisfying(AccountStoreException.class, e -> {
                    assertThat(e.getMessage()).contains("list accounts failed");
                    assertThat(e.getCause()).isSameAs(freeFailure);
                });
    }

    @Test
    void listWithinAttachesArrayFreeFailureAsSuppressedWhenQueryFails() {
        SQLException queryFailure = new SQLException("count query failed");
        SQLException freeFailure = new SQLException("ids array free failed");
        Array idsArray = proxyArray(freeFailure);
        Connection connection = proxyConnection(idsArray, queryFailure);
        JdbcAccountStore store = new JdbcAccountStore();

        assertThatThrownBy(() -> store.listWithin(
                connection,
                List.of("acct-1"),
                AccountStatus.ACCOUNT_STATUS_UNSPECIFIED,
                10,
                0L))
                .isInstanceOfSatisfying(AccountStoreException.class, e -> {
                    assertThat(e.getMessage()).contains("list accounts failed");
                    assertThat(e.getCause()).isSameAs(queryFailure);
                    assertThat(e.getCause().getSuppressed()).containsExactly(freeFailure);
                });
    }

    private static Array proxyArray(SQLException freeFailure) {
        return (Array) Proxy.newProxyInstance(
                Array.class.getClassLoader(),
                new Class<?>[]{Array.class},
                (proxy, method, args) -> {
                    if ("free".equals(method.getName())) {
                        if (freeFailure != null) {
                            throw freeFailure;
                        }
                        return null;
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
    }

    private static Connection proxyConnection(Array idsArray, SQLException queryFailure) {
        AtomicInteger statementCount = new AtomicInteger();
        return (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class<?>[]{Connection.class},
                (proxy, method, args) -> {
                    if ("createArrayOf".equals(method.getName())) {
                        return idsArray;
                    }
                    if ("prepareStatement".equals(method.getName())) {
                        boolean isCount = statementCount.getAndIncrement() == 0;
                        return proxyStatement(isCount, queryFailure);
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
    }

    private static PreparedStatement proxyStatement(boolean isCount, SQLException queryFailure) {
        return (PreparedStatement) Proxy.newProxyInstance(
                PreparedStatement.class.getClassLoader(),
                new Class<?>[]{PreparedStatement.class},
                (proxy, method, args) -> {
                    String name = method.getName();
                    if ("setArray".equals(name)
                            || "setInt".equals(name)
                            || "setLong".equals(name)
                            || "setString".equals(name)
                            || "close".equals(name)) {
                        return null;
                    }
                    if ("executeQuery".equals(name)) {
                        if (queryFailure != null) {
                            throw queryFailure;
                        }
                        return proxyResultSet(isCount);
                    }
                    throw new UnsupportedOperationException(name);
                });
    }

    private static ResultSet proxyResultSet(boolean isCount) {
        return (ResultSet) Proxy.newProxyInstance(
                ResultSet.class.getClassLoader(),
                new Class<?>[]{ResultSet.class},
                (proxy, method, args) -> {
                    String name = method.getName();
                    if ("next".equals(name)) {
                        return isCount;
                    }
                    if ("getLong".equals(name)) {
                        return 0L;
                    }
                    if ("close".equals(name)) {
                        return null;
                    }
                    throw new UnsupportedOperationException(name);
                });
    }
}
