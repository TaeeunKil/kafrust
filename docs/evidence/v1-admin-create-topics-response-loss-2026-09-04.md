# CreateTopics response-loss classification (2026-09-04)

## Scope

- source commit: `7614cb9d2fe7ce349105eae146a39657a5aaa422`
- test: `admin::tests::classifies_create_topics_response_loss_after_transmission`
- topology: in-memory scripted broker
- API: CreateTopics v2

## Checks

The scripted broker returned controller metadata, read the complete
CreateTopics request, and then closed the connection without sending a
response. The client had no mutation retry budget and returned the typed
`Error::AdminMutationOutcomeUnknown { operation: "CreateTopics" }` error. The
fixture asserts the request API key/version and reads the full frame before the
disconnect, proving that the ambiguity boundary is post-transmission and that
the client does not blindly replay the write.

## Boundary

Required Windows validation passed: formatting, workspace check, all-features
tests, Clippy with `-D warnings`, documentation, and whitespace checks. This is
scripted local evidence only; other controller mutations, published floor and
authorization profiles, three-broker failover, long campaigns, service canary,
and release authorization remain open.
