# Published Security Refresh — 2026-09-04

This record captures four bounded security smokes executed from the pushed
`b3ccbd6b87d55e77c84ccfca78e9e388f33239b1` source revision against the
published `kafrust 0.3.6` artifact. The external projects were created outside
the repository and resolved `kafrust`/`kafrust-protocol` from crates.io.

## Results

| Run | Profile | Result | Job duration |
| --- | --- | --- | --- |
| [33840641199](https://github.com/TaeeunKil/kafrust/actions/runs/33840641199) | Kafka 3.7.2, mutual TLS | passed | 68 seconds |
| [33840643890](https://github.com/TaeeunKil/kafrust/actions/runs/33840643890) | Kafka 4.3.1, mutual TLS | passed | 86 seconds |
| [33840646148](https://github.com/TaeeunKil/kafrust/actions/runs/33840646148) | Kafka 3.7.2, SASL_SSL/OAUTHBEARER | passed | 65 seconds |
| [33840648581](https://github.com/TaeeunKil/kafrust/actions/runs/33840648581) | Kafka 3.7.2, signed SASL_SSL/OAUTHBEARER | passed | 100 seconds |

## Assertions

The two mutual-TLS runs generated a short-lived CA, broker certificate, and
client certificate, required client authentication, and passed the published
client's Admin lifecycle, direct produce/consume, transaction commit/abort
with `read_committed`, and group commit/restore checks. The client output
reported `published mTLS passed` for both broker versions.

The unsigned OAUTHBEARER run validated provider-backed authentication,
broker discovery, produce, consume, and the published lockfile version check;
the output included `published oauthbearer ok`, `token_provider=true`, and
`consumed=true`.

The signed OAUTHBEARER run validated the local OIDC/JWKS endpoint and the
RS256 token claims (`issuer`, `audience`, and `subject`) before running the
published client. It then passed produce/consume and a separate
re-authentication project. The re-authentication output reported
`provider_calls=2`, `sasl_auth_version=1`, and `same_connection=true`; the
published lockfile version check also passed.

Generated credentials and bearer tokens were used only inside the ephemeral
workflow environment. No credential material or broker diagnostics were
uploaded by these successful runs.

## Qualification boundary

These four rows are bounded published-artifact evidence for mTLS and
OAUTHBEARER connectivity. They do not establish credential rotation,
provider outage or expiry handling, JWKS key rollover, restricted-principal
authorization, a complete mechanism matrix, long-duration behavior, service
canary readiness, or release authorization. V1-16 therefore remains
`In progress`.
