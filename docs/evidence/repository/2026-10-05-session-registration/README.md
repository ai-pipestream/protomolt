# Internal durable publication registration

The private journaled session factory retains a claim token alongside its session
seeds before SQL. Admission requires fixed modes, then commits exact claim
acquisition, preparation, modes and claimed-owner admission in order. No provider
work happens in this registration path. Retrying preserves identities and never
renews the claim lease implicitly.

Real PostgreSQL tests inject SQL acknowledgment loss after each of those four
durable stages. They verify owner absence before owner admission, the original
claim token and deadline, owner nonce, attempt IDs, upload tokens and stored modes
after retry. Missing modes and scoped callers are refused; journal byte leases
drain after faults and success.

`:protomolt-repo-container:test --tests '*DocumentPublicationSessionIT'` includes
13 cases covering these checks and existing session/recovery behavior.

Inventory: private Java session construction extended; no protobuf change or
public endpoint added. Ordinary runtime construction is still unjournaled. This
factory requires actual process authority; it never creates a privileged caller
from scoped credentials. Durable assessment-start integration, scoped host/journal
authority separation, denial retention policy and successor recovery remain open.
