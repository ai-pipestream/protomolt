# CLAUDE.md

Read and follow [AGENTS.md](AGENTS.md), including its repository architecture,
data integrity and remote policies. These rules apply to every module.

Read its **Start here: reuse and scope control** section before editing or
delegating. It is the single source of agent policy; do not maintain a conflicting
Claude-specific architecture. Consult the linked toolkit audit before inventing
replacement helpers. Existing implementations are not permission to copy defects.

Current scope is reconciliation/audit on `reconcile/parts-based-repository`.
Main and `ai-slop-archive-by-astra` remain untouched. Audit completion is not
implementation authorization. Carry these constraints into every worker handoff.

The canonical document API is the parts-based `DocumentService`; see
[the repository guide](docs/repo/publication.md). Do not revive the removed
publication transport from historical design documents or test evidence.
