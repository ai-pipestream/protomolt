package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import java.util.HexFormat;
import java.util.Objects;
import java.util.UUID;

/** Private retention completion, never an execution, content-read or provider-delete capability. */
final class DocumentPreparationRootReleases {
    enum Coverage { UNKNOWN, LIVE_EXACT, RELEASED_EXACT }
    record Receipt(RepositoryOperationLedger.Key key,long predecessorGeneration,String preparationSha256,
            int rootCount,String rootsSha256,int captureCount,String capturesSha256,
            DocumentPreparationTerminalEvidence.Evidence terminal,String creationXid) {
        @Override public String toString() { return "PreparationRootRelease[private]"; }
    }
    private record State(Coverage coverage,Receipt receipt) {}
    private final DocumentPublicationPreparationRecord record;
    private final String preparationSha;
    private final DocumentPreparationTerminalEvidence terminal;

    private DocumentPreparationRootReleases(DocumentPublicationPreparationRecord record) {
        this.record=record;
        preparationSha=hex(DocumentPublicationPreparationJournal.digest(DocumentPublicationPreparationCodec.encode(record)));
        terminal=new DocumentPreparationTerminalEvidence(record);
    }

    /** Reserves before encoding; retries inspect permanent evidence without resolving source revisions. */
    static Receipt release(Tx tx,PayloadBudget budget,RepositoryCaller caller,
            DocumentPublicationPreparationRecord record,RepositoryReadControl control) {
        Objects.requireNonNull(control).check(); Objects.requireNonNull(tx); Objects.requireNonNull(record);
        DocumentAdmissionAuthorization.requireCaller(caller,record.key(),record.key().account());
        if (!caller.processAuthority()) throw new RepositoryException(RepositoryException.Code.PERMISSION_DENIED,
                "Preparation release requires private process authority");
        try (var memory=budget.reserve(3L*DocumentPublicationPreparationCodec.MAX_BYTES+1024*1024)) {
            var release=new DocumentPreparationRootReleases(record);
            var coverage=DocumentPreparationCaptureCoverage.prepare(record,control);
            control.check();
            return tx.inTransaction(em -> {
                var outcome=release.terminal.lockAndRequire(em,caller,control);
                var state=release.inspect(em,outcome);
                if (state.coverage()==Coverage.UNKNOWN) throw incomplete("Preparation retention coverage is unknown");
                if (state.coverage()==Coverage.RELEASED_EXACT) { control.check(); return state.receipt(); }
                int captures=coverage.lockAndRequireDrained(em,control);
                release.insert(em,outcome,captures);
                int deleted=release.scope(em.createNativeQuery("""
                        DELETE FROM repository_preparation_history_roots
                        WHERE account_id=:a AND principal=:p AND operation_id=:o AND predecessor_generation=:g
                        """)).executeUpdate();
                if (deleted!=DocumentPreparationHistoryRoots.roots(record.command()).size()) throw corrupt();
                var completed=release.inspect(em,outcome);
                if (completed.coverage()!=Coverage.RELEASED_EXACT) throw corrupt();
                control.check();
                // No post-commit cancellation check: a completed release is not rolled back by a late signal.
                return completed.receipt();
            });
        }
    }

    /** Terminal inspector already holds claim and exact V81 locks. Header is the next lock. */
    private State inspect(EntityManager em,DocumentPreparationTerminalEvidence.Evidence outcome) {
        var headers=scope(em.createNativeQuery("""
                SELECT preparation_sha256,command_sha256,expected_count,roots_sha256,sealed
                FROM repository_preparation_history_sets
                WHERE account_id=:a AND principal=:p AND operation_id=:o AND predecessor_generation=:g FOR UPDATE
                """)).getResultList();
        if (headers.isEmpty()) return new State(Coverage.UNKNOWN,null);
        var header=(Object[])headers.getFirst();
        var roots=DocumentPreparationHistoryRoots.roots(record.command());
        String rootsSha=hex(DocumentPreparationHistoryRoots.digest(roots));
        if (!preparationSha.equals(hex(header[0])) || !record.command().sha256().equals(hex(header[1]))
                || roots.size()!=((Number)header[2]).intValue() || !rootsSha.equals(hex(header[3]))
                || !Boolean.TRUE.equals(header[4])) throw corrupt();
        var rows=scope(em.createNativeQuery("""
                SELECT preparation_sha256,command_sha256,root_count,roots_sha256,capture_count,captures_sha256,
                  terminal_kind,terminal_generation,terminal_sha256,terminal_xid::text,
                  abandonment_token,abandonment_nonce,creation_xid::text
                FROM repository_preparation_root_releases
                WHERE account_id=:a AND principal=:p AND operation_id=:o AND predecessor_generation=:g
                """)).getResultList();
        if (rows.isEmpty()) {
            if (DocumentPreparationHistoryRoots.coverage(em,record,HexFormat.of().parseHex(preparationSha))
                    !=DocumentPreparationHistoryRoots.Coverage.EXACT) throw corrupt();
            return new State(Coverage.LIVE_EXACT,null);
        }
        var row=(Object[])rows.getFirst();
        var evidence=terminal(row);
        int captures=((Number)row[4]).intValue();
        if (!preparationSha.equals(hex(row[0])) || !record.command().sha256().equals(hex(row[1]))
                || roots.size()!=((Number)row[2]).intValue() || !rootsSha.equals(hex(row[3]))
                || captures<1 || captures>16 || !outcome.equals(evidence)) throw corrupt();
        var retained=(Object[])scope(em.createNativeQuery("""
                SELECT (SELECT count(*) FROM repository_preparation_history_roots
                  WHERE account_id=:a AND principal=:p AND operation_id=:o AND predecessor_generation=:g),
                 (SELECT count(*) FROM repository_preparation_pin_batches
                  WHERE account_id=:a AND principal=:p AND operation_id=:o AND predecessor_generation=:g),
                 repository_preparation_capture_fingerprint(:a,:p,:o,:g)
                """)).getSingleResult();
        if (((Number)retained[0]).longValue()!=0 || ((Number)retained[1]).longValue()!=captures
                || !hex(retained[2]).equals(hex(row[5]))) throw corrupt();
        return new State(Coverage.RELEASED_EXACT,new Receipt(record.key(),record.predecessorGeneration(),preparationSha,
                roots.size(),rootsSha,captures,hex(row[5]),evidence,(String)row[12]));
    }

    private void insert(EntityManager em,DocumentPreparationTerminalEvidence.Evidence outcome,int captures) {
        String alternative;
        if (outcome instanceof DocumentPreparationTerminalEvidence.Outcome) {
            alternative="terminal_generation,terminal_sha256,terminal_xid";
        } else alternative="abandonment_token,abandonment_nonce";
        String values=outcome instanceof DocumentPreparationTerminalEvidence.Outcome
                ? ":generation,:result,CAST(:xid AS xid8)" : ":token,:nonce";
        var q=scope(em.createNativeQuery("""
                INSERT INTO repository_preparation_root_releases(account_id,principal,operation_id,predecessor_generation,
                  preparation_sha256,command_sha256,root_count,roots_sha256,capture_count,captures_sha256,terminal_kind,
                """+alternative+") SELECT account_id,principal,operation_id,predecessor_generation,"+"""
                  preparation_sha256,command_sha256,expected_count,roots_sha256,:captures,
                  repository_preparation_capture_fingerprint(account_id,principal,operation_id,predecessor_generation),:kind,
                """+values+" FROM repository_preparation_history_sets "+"""
                WHERE account_id=:a AND principal=:p AND operation_id=:o AND predecessor_generation=:g
                """)).setParameter("captures",captures);
        if (outcome instanceof DocumentPreparationTerminalEvidence.Outcome result) {
            q.setParameter("kind",result.kind()).setParameter("generation",result.generation())
                    .setParameter("result",HexFormat.of().parseHex(result.resultSha256())).setParameter("xid",result.creationXid());
        } else {
            var abandoned=(DocumentPreparationTerminalEvidence.Abandonment)outcome;
            q.setParameter("kind","ABANDONMENT").setParameter("token",abandoned.claimToken()).setParameter("nonce",abandoned.ownerNonce());
        }
        if (q.executeUpdate()!=1) throw corrupt();
    }

    private static DocumentPreparationTerminalEvidence.Evidence terminal(Object[] row) {
        if ("ABANDONMENT".equals(row[6])) return new DocumentPreparationTerminalEvidence.Abandonment((UUID)row[10],(UUID)row[11]);
        if (!"SUCCESS".equals(row[6]) && !"REJECTION".equals(row[6])) throw corrupt();
        return new DocumentPreparationTerminalEvidence.Outcome((String)row[6],((Number)row[7]).longValue(),hex(row[8]),(String)row[9]);
    }
    private Query scope(Query q) {
        return q.setParameter("a",record.key().account()).setParameter("p",record.key().principal())
                .setParameter("o",record.key().operationId()).setParameter("g",record.predecessorGeneration());
    }
    private static String hex(Object bytes) { return HexFormat.of().formatHex((byte[])bytes); }
    private static RepositoryException corrupt() { return new RepositoryException(RepositoryException.Code.DATA_LOSS,
            "Preparation release evidence differs from retained canonical identity"); }
    private static RepositoryException incomplete(String message) { return new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION,message); }
}
