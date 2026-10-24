# CLAUDE.md

Read and follow [AGENTS.md](AGENTS.md), including its repository architecture,
data integrity and remote policies. These rules apply to every module.

Read its **Start here: reuse and scope control** and **Protobuf toolkit & zero-regex
quick reference** sections before editing or delegating. It is the single source of
agent policy; do not maintain a conflicting Claude-specific architecture. Consult
the toolkit quick reference and linked audit before inventing replacement helpers.
Never round-trip protobuf through JSON or `Struct` for internal manipulation or
validation, and never introduce `java.util.regex` in production Java sources.
Existing implementations are not permission to copy defects.

Cut issue and feature branches from `main` and submit work via pull requests. Leave
`main` (direct pushes) and `ai-slop-archive-by-astra` untouched. Carry these
constraints into every worker handoff.

The canonical document API is the parts-based `DocumentService`; see
[the repository guide](docs/repo/publication.md). Do not revive the removed
publication transport from historical design documents or test evidence.
