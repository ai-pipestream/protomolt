package ai.protomolt.proto.intake.service.identity;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.reflect.Proxy;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.junit.jupiter.api.Test;

/**
 * Unit tests verifying that {@link JdbcApiKeyIdentityResolver#textArray} frees
 * the JDBC {@link Array} without swallowing cleanup failures or masking
 * primary extraction failures.
 */
class JdbcApiKeyIdentityResolverArrayTest {

    @Test
    void textArrayPropagatesFreeFailureWhenGetArraySucceeds() {
        SQLException freeFailure = new SQLException("array free failed");
        Array sqlArray = proxyArray(new String[]{"ds-1", "ds-2"}, null, freeFailure);
        ResultSet row = proxyResultSet(sqlArray);

        assertThatThrownBy(() -> JdbcApiKeyIdentityResolver.textArray(row, "datasource_ids"))
                .isSameAs(freeFailure);
    }

    @Test
    void textArrayAttachesFreeFailureAsSuppressedWhenGetArrayThrows() {
        SQLException primary = new SQLException("array materialization failed");
        SQLException freeFailure = new SQLException("array free failed");
        Array sqlArray = proxyArray(null, primary, freeFailure);
        ResultSet row = proxyResultSet(sqlArray);

        assertThatThrownBy(() -> JdbcApiKeyIdentityResolver.textArray(row, "datasource_ids"))
                .isSameAs(primary)
                .satisfies(t -> assertThat(t.getSuppressed()).containsExactly(freeFailure));
    }

    private static ResultSet proxyResultSet(Array array) {
        return (ResultSet) Proxy.newProxyInstance(
                ResultSet.class.getClassLoader(),
                new Class<?>[]{ResultSet.class},
                (proxy, method, args) -> {
                    if ("getArray".equals(method.getName())) {
                        return array;
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
    }

    private static Array proxyArray(
            String[] values, SQLException getArrayFailure, SQLException freeFailure) {
        return (Array) Proxy.newProxyInstance(
                Array.class.getClassLoader(),
                new Class<?>[]{Array.class},
                (proxy, method, args) -> {
                    if ("getArray".equals(method.getName())) {
                        if (getArrayFailure != null) {
                            throw getArrayFailure;
                        }
                        return values;
                    }
                    if ("free".equals(method.getName())) {
                        if (freeFailure != null) {
                            throw freeFailure;
                        }
                        return null;
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
    }
}
