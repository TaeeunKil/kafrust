# V1-13 Bootstrap And Security Admin Mutations

- Status: In progress
- Target evidence: Published artifact
- Dependencies: V1-02, V1-11

## User-Visible Objective

Stabilize security-sensitive configuration, ACL, quota, credential, and
delegation-token Admin behavior through each operation's actual bootstrap or
controller route, with safe pre-send retry, typed partial authorization
results, secret redaction, and explicit post-send reconciliation.

## Non-Goals

- No bundled identity-provider, secret store, or credential rotator.
- No GSSAPI/Kerberos or cloud IAM unless V1-01 explicitly adds them.
- No logging of ACL credentials, SCRAM material, tokens, HMACs, or private keys.
- No blind response-loss replay of a security mutation.

## Scope

- `crates/kafrust/src/{admin,client,config,scram,error}.rs`
- AlterConfigs 33, IncrementalAlterConfigs 44, DescribeConfigs 32,
  Describe/Create/Delete ACLs 29/30/31, Describe/AlterClientQuotas 48/49,
  Describe/AlterUserScramCredentials 50/51 as allocated by the routing ledger,
  and delegation token Create/Describe/Renew/Expire 38-41
- APIs 51 and 38-41 are controller-routed in current source; V1-13 is their
  single behavior/evidence owner and reuses V1-11's controller-routing contract
- TLS/SASL/mTLS channel prerequisites and authorization identities
- Admin security examples, response-drop tests, restricted-user workflows, and
  release/migration/security documentation

## Work Packages

1. Classify each method's route, idempotence, secret fields, partial-result
   shape, authorization resource, and reconciliation read.
2. Test the allocated route owner's discovery/connection failure before
   transmission and response loss after transmission for every mutation.
3. Cover allowed/denied mixed batches without losing entry order or Kafka
   errors.
4. Verify redacted Debug/error/tracing/metrics output for passwords, SCRAM salted
   material, bearer tokens, delegation HMACs, certificates, and private keys.
5. Qualify delegation token renewal/expiry authorization and reconciliation on
   broker versions that enable the required secret configuration.
6. Run published restricted-principal and administrator profiles.

## Current Execution Record (2026-08-22)

V1-13 is now `In progress`. The security Admin surface keeps route ownership
explicit for configs, ACLs, quotas, SCRAM credentials, and delegation tokens.
Pre-send connection/authentication/controller discovery may retry; possible
post-send mutation response loss returns `AdminMutationOutcomeUnknown` without
replay. Mixed allow/deny and per-entry results remain typed and ordered.

Local tests cover SCRAM exchange/error redaction, TLS/mTLS configuration
validation, ACL and quota partial results, controller-routed SCRAM credential
operations, delegation-token version negotiation, and response-loss
classification. Debug/error/tracing paths do not print credential material.
Restricted-principal live profiles, delegation-token authorization, and the
zero-secret-marker artifact scan remain open; no security compatibility claim
is made beyond the existing published baseline.

### Seeded artifact scan (2026-09-03)

`scripts/check_v1_secret_artifacts.py` now scans retained `docs/evidence` files
for the seven deterministic credential markers used by the redaction tests.
It reads in bounded chunks, detects markers split across chunk boundaries, and
reports only marker indexes so a finding cannot echo the secret. The local
scan covered 47 files with zero findings; the checker and its four unit tests
are wired into the required CI. This closes the deterministic artifact-scan
slice only. Restricted-principal/delegation-token live profiles and the
published security rows remain open.

## Failure And Lifecycle Contract

- Pre-send connect/authentication/controller-discovery failures may retry using
  the operation's allocated route inside the Admin budget.
- After possible transmission, non-idempotent mutations return
  `AdminMutationOutcomeUnknown` and do not replay.
- Authentication failure, authorization denial, and per-entry validation remain
  distinct typed results.
- A failed security mutation does not downgrade the channel or expose secret
  material.
- Cancellation after write uses the same ambiguity boundary; cleanup errors do
  not overwrite a more important unknown outcome.

## Verification

- Deterministic before/after-transmission and mixed partial-result tests for
  every mutation family.
- Snapshot/property tests scan Debug, Display, tracing fields, and uploaded test
  artifacts for seeded secret markers; expected count is zero.
- Published accepted-floor and pinned-current SASL_SSL/SCRAM-SHA-256 restricted
  and admin profiles run configs, ACLs, quotas, SCRAM, and enabled delegation
  token lifecycle operations.
- Routing assertions prove APIs 51 and 38-41 reach the active controller while
  bootstrap-routed operations remain on their allocated broker route.
- Every unknown mutation has a documented read/reconcile step and one request.

## Exit Criteria

1. All allocated methods have route/idempotence/secret/reconciliation entries.
2. Every response-loss mutation returns one typed unknown outcome without replay.
3. Allowed/denied batch entries remain complete and ordered.
4. Secret-marker leakage count is zero across local and workflow artifacts.
5. Both published security profiles pass and docs/ledger are complete.

## Migration And Rollback

Map rust-rdkafka Admin options and per-entry results without copying secret
ownership patterns blindly. Rotate or revoke credentials through operator-owned
systems. Rollback code only after reading the actual broker state; never restore
a secret by writing a value recovered from logs.

## Conventional Commit Plan

1. `test(admin): cover security mutation ambiguity and redaction`
2. `fix(admin): preserve partial authorization outcomes`
3. `ci(admin): qualify published restricted security operations`
4. `docs(security): add Admin reconciliation guidance`

## Evidence Record On Completion

Record operation/API/version, principal class, allow/deny totals, fault point,
unknown/reconcile result, redaction scan count, broker configuration, artifact,
and provider/mechanism non-claims without recording secrets.
