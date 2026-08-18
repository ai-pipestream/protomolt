# Screening

Status: design of record for model-driven screening (PII and entity
detection), decided 2026-08-18 with the project owner. Nothing here is
implemented; landing any of it means updating this chapter first. The
hard line it rests on is stated in
[well-known types](well-known-types.md) ("Screening is not
validation") and is restated here because every design decision below
follows from it.

## The hard line

Deterministic checks are **schema truth**: same input, same verdict,
forever. That is what makes them safe to federate, compat-gate, and
enforce at every door — the whole validate.v1 stance. Model-driven
detection is **probabilistic and model-versioned**: the same input can
change verdicts on a model update with no schema change anywhere. A
screening verdict must therefore never masquerade as schema validity.
No screening result ever rides a validate.v1 rule, and no schema
compat gate ever depends on a model.

What the schema declares is *that* a field is screened, not what the
screening decides: the declaration rides the `meta.v1` sensitivity
classes ("screened as PII"), which are already descriptor metadata the
platform surfaces (the metric mapping carries sensitivity today). The
declaration is durable schema truth; the verdicts are runtime facts
with evidence.

## Mount configuration, through the config lane

The model, its thresholds, and the policy are **mount configuration**,
distributed as typed config documents on the
[config lane](config-distribution.md) exactly like routing rules and
taxonomies: registry-stored, verify-then-swap, the source's version as
evidence. Swapping a model or tightening a threshold is a config
publish, never a schema change and never a redeploy. A node can always
say which screening config it runs, the same way it says which routing
rules it runs.

## Policy: mask or tag, rarely refuse

Screening's default posture is **mask-or-tag, not refuse**. The
platform already owns the masking machinery (`mask-message`,
part-masked documents), so "PII-aware doors" is a policy on existing
verbs: a screened field's detected spans mask on the way through, or
the document tags what was found, and only an explicit policy refuses.
Refusal-by-default would make a probabilistic false positive into an
outage; masking makes it a redaction that evidence can audit.

Every mask, tag, or refusal carries the **model version and threshold
as evidence**, the same stance as `physical_plan` and config versions:
a verdict without the model that produced it is not auditable, and two
nodes running different models must be distinguishable from their
output.

## The OpenNLP boundary

Apache OpenNLP is the engine (NER/PII detection; the project owner is
an OpenNLP 3.x committer, and the house fork is battle-testing fixes
pre-upstream). The dependency boundary holds firm:

- The screening layer depends on OpenNLP — that is its job.
- `core/formats` and the validation core stay **zero-dependency**.
  Where OpenNLP's regex-free string utilities cover a deterministic
  format, the approach is ported (Apache to Apache, NOTICE attribution
  when code moves), never the jar added: every dependency is a
  CVE-response burden on every consumer.
- Model artifacts are data, not code: fetched and mounted like any
  operator-supplied pack, never bundled in-tree (the same
  Apache-clean licensing rule the taxonomy chapter enforces).

## Sequencing

Screening lands after the search-first product surface is proven; the
chapter exists so that when it lands, it lands on these rails instead
of inventing new ones. The first implementation slice, when scheduled:
one screened sensitivity class, one mounted model config document, the
mask policy on one door, and the model version in the mask evidence.
