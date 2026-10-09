package ai.protomolt.proto.repo.container.ledger;

/** Controlled gates and faults around actual PostgreSQL commit; other JDBC calls delegate. */
final class DocumentJdbcFaults {
    private DocumentJdbcFaults() {}
    @FunctionalInterface interface CommitResponse { void run() throws java.sql.SQLException; }
    @FunctionalInterface interface CommitRequest { void run(java.sql.Connection connection) throws java.sql.SQLException; }

    static javax.sql.DataSource afterCommit(javax.sql.DataSource delegate, CommitResponse responseFault) {
        return aroundCommit(delegate, connection -> {}, responseFault);
    }

    static javax.sql.DataSource beforeCommit(javax.sql.DataSource delegate, CommitRequest requestFault) {
        return aroundCommit(delegate, requestFault, () -> {});
    }

    private static javax.sql.DataSource aroundCommit(javax.sql.DataSource delegate, CommitRequest requestFault, CommitResponse responseFault) {
        return (javax.sql.DataSource) java.lang.reflect.Proxy.newProxyInstance(
                javax.sql.DataSource.class.getClassLoader(), new Class<?>[]{javax.sql.DataSource.class}, (proxy, method, args) -> {
                    final Object result;
                    try { result = method.invoke(delegate, args); }
                    catch (java.lang.reflect.InvocationTargetException failure) { throw failure.getCause(); }
                    if (!method.getName().equals("getConnection")) return result;
                    var connection = (java.sql.Connection) result;
                    return java.lang.reflect.Proxy.newProxyInstance(java.sql.Connection.class.getClassLoader(),
                            new Class<?>[]{java.sql.Connection.class}, (connectionProxy, operation, parameters) -> {
                                if (operation.getName().equals("commit")) requestFault.run(connection);
                                final Object response;
                                try { response = operation.invoke(connection, parameters); }
                                catch (java.lang.reflect.InvocationTargetException failure) { throw failure.getCause(); }
                                if (operation.getName().equals("commit")) responseFault.run();
                                return response;
                            });
                });
    }
}
