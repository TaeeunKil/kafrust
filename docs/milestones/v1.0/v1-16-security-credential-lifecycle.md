# V1-16 Security And Credential Lifecycle

- Status: In progress
- Target evidence: Published artifact
- Dependencies: V1-15

## User-Visible Objective

Keep TLS, mTLS, SASL/PLAIN, SCRAM-SHA-256/512, and OAUTHBEARER credentials
confidential and rotate/re-authenticate them through bounded, observable
connection replacement without weakening the configured security protocol.

## Non-Goals

- No bundled HTTP/OIDC client, certificate authority, or secret manager.
- No GSSAPI/Kerberos, AWS MSK IAM, or other provider claim unless V1-01 adds it.
- No plaintext fallback after TLS/SASL failure.
- No claim that the optional TLS dependency profile needs no native build
  tooling; V1-19 defines that feature-specific posture.

## Scope

- `crates/kafrust/src/{config,client,scram,error,producer,consumer,group,
  share_consumer,streams,admin,telemetry}.rs`
- SecurityProtocol, SaslMechanism/Credentials, TLS roots/server name, DER client
  certificate/private key, OAuth token/provider/source/cache, SASL handshake and
  authenticate versions, re-authentication, bootstrap rotation, and Debug/error
  redaction
- local signed OIDC/JWKS fixtures, mTLS workflows, main live matrix, published
  OAuth/mTLS/security workflows, migration and security documentation

## Work Packages

1. Freeze supported mechanism/config names, validation, time budgets, secret
   ownership, clone behavior, and redacted formatting.
2. Add deterministic certificate, private-key, SCRAM password, bearer token,
   expiry, authorization identity, and provider error boundaries.
3. Rotate server trust, client certificate/key, SCRAM credentials, and signed
   OAuth key/token without process restart where the stable API promises it;
   otherwise document restart-required behavior explicitly.
4. Cover provider timeout/outage, expired token, JWKS key rollover in the local
   fixture, re-auth challenge/error acknowledgement, and connection discard.
5. Run single- and multi-broker floor/current secure profiles from the exact
   artifact.
6. Scan all logs, traces, errors, Debug output, and uploaded artifacts for seeded
   secrets.

## Current Execution Record (2026-08-22)

V1-16 is now `In progress`. The current source has deterministic coverage for
SCRAM-256/512 message construction, TLS/mTLS material pairing and validation,
SASL/PLAIN and OAUTHBEARER handshake boundaries, provider single-flight and
refresh-window rotation, valid-token use during a source outage, provider
timeouts, invalid server-final handling, and password/token redaction in
Debug/error paths. The local signed OIDC/JWKS fixture and the mTLS workflow
provide reusable live-gate inputs, while the published signed-OAuth and mTLS
profiles remain artifact-specific evidence rather than current-source proof.

The security contract is deliberately conservative: an authentication or
provider failure poisons the affected connection, never downgrades the
configured protocol, and does not expose credential material. Rotation,
server/key replacement, restricted-principal authorization, and the seeded
zero-match scan across logs and uploaded artifacts remain open. The required
floor/current published profiles and final provider/connection gauges have not
yet been run for the coordinated `0.3.6` candidate.

### Published 0.3.6 security refresh (2026-08-23)

The exact published `0.3.6` pair passed fresh mutual-TLS smoke on Kafka 3.7.2
and 4.3.1 in
[32646371786](https://github.com/TaeeunKil/kafrust/actions/runs/32646371786)
and [32646373388](https://github.com/TaeeunKil/kafrust/actions/runs/32646373388).
The external project covered Admin lifecycle, direct produce/consume,
transaction commit/abort with `read_committed`, and group commit/restore. The
published OAUTHBEARER smoke also passed in
[32646374747](https://github.com/TaeeunKil/kafrust/actions/runs/32646374747),
including the provider and consumed-record assertions with the published
lockfile version check. These are short published rows only; credential
rotation, provider outage/expiry, restricted-principal, zero-secret scan, and
the complete V1-16 floor/current matrix remain open.

## Failure And Lifecycle Contract

- Authentication and provider calls are bounded by the configured request or
  explicit credential-refresh budget.
- A failed/expired credential never returns the connection to a cache and never
  downgrades security.
- Concurrent cached OAuth refresh is single-flight; callers receive typed source
  or authentication failures without the token value.
- Re-authentication keeps Kafka's same-connection requirements where applicable
  and replaces the socket after terminal failure.
- Cancellation zeroizes feature-supported secret buffers where promised and
  releases provider waiters.
- Certificate/key pairs validate atomically; incomplete rotation is rejected.

## Verification

- Deterministic handshake byte/order tests for PLAIN, SCRAM-256/512, and OAuth;
  certificate pairing and redaction tests; provider concurrency/timeouts;
  expired/future tokens; re-auth failure; rotation; cancellation and close.
- Seeded secret scan across all local/workflow outputs has zero matches.
- Published accepted-floor and pinned-current profiles cover TLS, mTLS,
  SASL_PLAINTEXT/PLAIN, SASL_SSL/SCRAM-256/512 where supported, built-in OAuth,
  and signed local OIDC/JWKS; at least the pinned-current secured profile is
  three-broker with leader/coordinator movement during rotation.
- External provider profiles remain unclaimed unless V1-01 names and authorizes
  an exact provider gate.

## Exit Criteria

1. Every supported mechanism has validation, failure, rotation/restart, and
   redaction contracts.
2. No tested failure downgrades the channel or reuses an unauthenticated socket.
3. All accepted published security profiles pass on the exact artifact.
4. Secret scan count is zero and final provider/connection gauges are zero.
5. Configuration, migration, compatibility, dependency posture, and ledger
   records agree.

## Migration And Rollback

Map security protocol/mechanism, CA/server name, certificate/key, username/
password, OAuth provider, and re-authentication settings. Rollback must keep the
old credential valid until new connections are confirmed or require an
explicit restart window. Never record credential material in rollback notes.

## Conventional Commit Plan

1. `test(security): cover credential rotation and provider faults`
2. `fix(security): preserve authenticated connection lifecycle`
3. `ci(security): qualify published rotation profiles`
4. `docs(security): define v1 credential lifecycle`

## Evidence Record On Completion

Record mechanisms and versions, rotation event type, provider/error class,
connection replacement/re-auth result, broker/topology, artifact, secret scan
count, final gauges, and provider/non-native-toolchain non-claims without secrets.
