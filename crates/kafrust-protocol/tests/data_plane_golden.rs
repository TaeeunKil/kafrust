//! Byte-auditable request fixtures for the V1 data-plane selection boundary.
//!
//! The field order and header versions in these fixtures are checked against
//! the Apache Kafka 4.3.1 message schemas referenced by
//! `docs/evidence/data-plane-version-manifest.json`.  Empty collections are
//! intentional: they isolate the selected header/body wire shape while the
//! record-batch and nullable-field tests remain in the API module suites.

// Encoding a fixed fixture is expected to be infallible for the constants in
// this file; unwrap keeps a failure pointed at the fixture that became invalid.
#![allow(clippy::unwrap_used)]

use kafrust_protocol::api::api_versions::{
    ApiVersionsRequestV0, ApiVersionsRequestV3, ApiVersionsRequestV4, ApiVersionsResponseV0,
    ApiVersionsResponseV3, ApiVersionsResponseV4,
};
use kafrust_protocol::api::fetch::{
    FetchRequestV11, FetchRequestV12, FetchRequestV13, FetchRequestV4, FetchResponseV11,
    FetchResponseV12, FetchResponseV13, FetchResponseV4,
};
use kafrust_protocol::api::list_offsets::{ListOffsetsRequestV1, ListOffsetsResponseV1};
use kafrust_protocol::api::metadata::{
    MetadataRequestV1, MetadataRequestV12, MetadataResponseV1, MetadataResponseV12,
};
use kafrust_protocol::api::offset_for_leader_epoch::{
    OffsetForLeaderEpochRequestV3, OffsetForLeaderEpochResponseV3,
};
use kafrust_protocol::api::produce::{
    ProduceRequestV11, ProduceRequestV12, ProduceRequestV13, ProduceRequestV2, ProduceRequestV3,
    ProduceRequestV7, ProduceRequestV9, ProduceResponseV13, ProduceResponseV2, ProduceResponseV7,
    ProduceResponseV9, ProduceTopicV13,
};
use kafrust_protocol::codec::Decoder;

fn assert_golden(actual: impl AsRef<[u8]>, expected: &[u8]) {
    assert_eq!(actual.as_ref(), expected);
}

#[test]
fn produce_selected_versions_have_stable_headers_and_empty_body_shapes() {
    let v2 = ProduceRequestV2 {
        correlation_id: 42,
        client_id: None,
        acks: 1,
        timeout_ms: 1_000,
        topics: Vec::new(),
    }
    .encode()
    .unwrap();

    assert_golden(
        &v2,
        &[
            0, 0, 0, 2, 0, 0, 0, 42, 255, 255, 0, 1, 0, 0, 3, 232, 0, 0, 0, 0,
        ],
    );

    for (version, expected) in [
        (
            3_i16,
            &[
                0, 0, 0, 3, 0, 0, 0, 42, 255, 255, 255, 255, 0, 1, 0, 0, 3, 232, 0, 0, 0, 0,
            ][..],
        ),
        (
            7_i16,
            &[
                0, 0, 0, 7, 0, 0, 0, 42, 255, 255, 255, 255, 0, 1, 0, 0, 3, 232, 0, 0, 0, 0,
            ][..],
        ),
    ] {
        let request = if version == 3 {
            ProduceRequestV3 {
                correlation_id: 42,
                client_id: None,
                transactional_id: None,
                acks: 1,
                timeout_ms: 1_000,
                topics: Vec::new(),
            }
            .encode()
            .unwrap()
        } else {
            ProduceRequestV7 {
                correlation_id: 42,
                client_id: None,
                transactional_id: None,
                acks: 1,
                timeout_ms: 1_000,
                topics: Vec::new(),
            }
            .encode()
            .unwrap()
        };
        assert_golden(&request, expected);
    }

    let expected_flexible = |version: i16| {
        vec![
            0,
            0,
            (version >> 8) as u8,
            version as u8,
            0,
            0,
            0,
            42,
            255,
            255,
            0,
            0,
            0,
            1,
            0,
            0,
            3,
            232,
            1,
            0,
        ]
    };

    let v11 = ProduceRequestV11 {
        correlation_id: 42,
        client_id: None,
        transactional_id: None,
        acks: 1,
        timeout_ms: 1_000,
        topics: Vec::new(),
    }
    .encode()
    .unwrap();
    assert_golden(&v11, &expected_flexible(11));

    let v9 = ProduceRequestV9 {
        correlation_id: 42,
        client_id: None,
        transactional_id: None,
        acks: 1,
        timeout_ms: 1_000,
        topics: Vec::new(),
    }
    .encode()
    .unwrap();
    assert_golden(&v9, &expected_flexible(9));

    let v12 = ProduceRequestV12 {
        correlation_id: 42,
        client_id: None,
        transactional_id: None,
        acks: 1,
        timeout_ms: 1_000,
        topics: Vec::new(),
    }
    .encode()
    .unwrap();
    assert_golden(&v12, &expected_flexible(12));

    let v13 = ProduceRequestV13 {
        correlation_id: 42,
        client_id: None,
        transactional_id: None,
        acks: 1,
        timeout_ms: 1_000,
        topics: vec![ProduceTopicV13 {
            topic_id: [7; 16],
            partitions: Vec::new(),
        }],
    }
    .encode()
    .unwrap();
    assert_eq!(&v13[..18], &expected_flexible(13)[..18]);
    assert_eq!(&v13[18], &2);
    assert_eq!(&v13[19..35], &[7; 16]);
    assert_eq!(&v13[35..], &[1, 0, 0]);
}

#[test]
fn fetch_selected_versions_have_stable_empty_collection_shapes() {
    let v4 = FetchRequestV4 {
        correlation_id: 42,
        client_id: None,
        replica_id: -1,
        max_wait_ms: 500,
        min_bytes: 1,
        max_bytes: 1_048_576,
        isolation_level: 0,
        topics: Vec::new(),
    }
    .encode()
    .unwrap();
    assert_golden(
        &v4,
        &[
            0, 1, 0, 4, 0, 0, 0, 42, 255, 255, 255, 255, 255, 255, 0, 0, 1, 244, 0, 0, 0, 1, 0, 16,
            0, 0, 0, 0, 0, 0, 0,
        ],
    );

    let v11 = FetchRequestV11 {
        correlation_id: 42,
        client_id: None,
        replica_id: -1,
        max_wait_ms: 500,
        min_bytes: 1,
        max_bytes: 1_048_576,
        isolation_level: 0,
        session_id: 0,
        session_epoch: -1,
        topics: Vec::new(),
        forgotten_topics: Vec::new(),
        rack_id: String::new(),
    }
    .encode()
    .unwrap();
    assert_golden(
        &v11,
        &[
            0, 1, 0, 11, 0, 0, 0, 42, 255, 255, 255, 255, 255, 255, 0, 0, 1, 244, 0, 0, 0, 1, 0,
            16, 0, 0, 0, 0, 0, 0, 0, 255, 255, 255, 255, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        ],
    );

    let v12 = FetchRequestV12 {
        correlation_id: 42,
        client_id: None,
        replica_id: -1,
        max_wait_ms: 500,
        min_bytes: 1,
        max_bytes: 1_048_576,
        isolation_level: 0,
        session_id: 0,
        session_epoch: -1,
        topics: Vec::new(),
        forgotten_topics: Vec::new(),
        rack_id: String::new(),
    }
    .encode()
    .unwrap();
    assert_golden(
        &v12,
        &[
            0, 1, 0, 12, 0, 0, 0, 42, 255, 255, 0, 255, 255, 255, 255, 0, 0, 1, 244, 0, 0, 0, 1, 0,
            16, 0, 0, 0, 0, 0, 0, 0, 255, 255, 255, 255, 1, 1, 1, 0,
        ],
    );

    let v13 = FetchRequestV13 {
        correlation_id: 42,
        client_id: None,
        cluster_id: None,
        replica_id: -1,
        max_wait_ms: 500,
        min_bytes: 1,
        max_bytes: 1_048_576,
        isolation_level: 0,
        session_id: 0,
        session_epoch: -1,
        topics: Vec::new(),
        forgotten_topics: Vec::new(),
        rack_id: String::new(),
    }
    .encode()
    .unwrap();
    let mut expected_v13 = v12.clone();
    expected_v13[3] = 13;
    assert_eq!(v13, expected_v13);
}

#[test]
fn metadata_offsets_and_api_versions_have_checked_request_fixtures() {
    assert_golden(
        MetadataRequestV1 {
            correlation_id: 42,
            client_id: None,
            topics: None,
        }
        .encode()
        .unwrap(),
        &[0, 3, 0, 1, 0, 0, 0, 42, 255, 255, 255, 255, 255, 255],
    );
    assert_golden(
        MetadataRequestV12 {
            correlation_id: 42,
            client_id: None,
            topics: None,
            allow_auto_topic_creation: true,
            include_topic_authorized_operations: false,
        }
        .encode()
        .unwrap(),
        &[0, 3, 0, 12, 0, 0, 0, 42, 255, 255, 0, 0, 1, 0, 0],
    );
    assert_golden(
        ListOffsetsRequestV1 {
            correlation_id: 42,
            client_id: None,
            replica_id: -1,
            topics: Vec::new(),
        }
        .encode()
        .unwrap(),
        &[
            0, 2, 0, 1, 0, 0, 0, 42, 255, 255, 255, 255, 255, 255, 0, 0, 0, 0,
        ],
    );
    assert_golden(
        OffsetForLeaderEpochRequestV3 {
            correlation_id: 42,
            client_id: None,
            replica_id: -1,
            topics: Vec::new(),
        }
        .encode()
        .unwrap(),
        &[
            0, 23, 0, 3, 0, 0, 0, 42, 255, 255, 255, 255, 255, 255, 0, 0, 0, 0,
        ],
    );

    assert_golden(
        ApiVersionsRequestV0 {
            correlation_id: 42,
            client_id: None,
        }
        .encode()
        .unwrap(),
        &[0, 18, 0, 0, 0, 0, 0, 42, 255, 255],
    );

    for (version, bytes) in [
        (
            3_i16,
            ApiVersionsRequestV3 {
                correlation_id: 42,
                client_id: None,
                client_software_name: "kafrust".to_owned(),
                client_software_version: "0.3.6".to_owned(),
            }
            .encode()
            .unwrap(),
        ),
        (
            4_i16,
            ApiVersionsRequestV4 {
                correlation_id: 42,
                client_id: None,
                client_software_name: "kafrust".to_owned(),
                client_software_version: "0.3.6".to_owned(),
            }
            .encode()
            .unwrap(),
        ),
    ] {
        let expected = [
            0,
            18,
            (version >> 8) as u8,
            version as u8,
            0,
            0,
            0,
            42,
            255,
            255,
            0,
            8,
            b'k',
            b'a',
            b'f',
            b'r',
            b'u',
            b's',
            b't',
            6,
            b'0',
            b'.',
            b'3',
            b'.',
            b'6',
            0,
        ];
        assert_eq!(bytes, expected);
    }
}

#[test]
fn selected_response_versions_have_stable_empty_body_shapes() {
    let mut decoder = Decoder::new(&[0, 0, 0, 0, 0, 0, 0, 0]);
    let response = ProduceResponseV2::decode_body(&mut decoder).unwrap();
    assert!(response.responses.is_empty());
    assert_eq!(response.throttle_time_ms, 0);
    assert!(decoder.is_empty());

    let mut decoder = Decoder::new(&[0, 0, 0, 0, 0, 0, 0, 0]);
    let response = ProduceResponseV7::decode_body(&mut decoder).unwrap();
    assert!(response.responses.is_empty());
    assert_eq!(response.throttle_time_ms, 0);
    assert!(decoder.is_empty());

    let mut decoder = Decoder::new(&[1, 0, 0, 0, 0, 0]);
    let response = ProduceResponseV9::decode_body(&mut decoder).unwrap();
    assert!(response.responses.is_empty());
    assert_eq!(response.throttle_time_ms, 0);
    assert!(response.node_endpoints.is_empty());
    assert!(decoder.is_empty());
    let mut decoder = Decoder::new(&[1, 0, 0, 0, 0, 0]);
    let response = ProduceResponseV13::decode_body(&mut decoder).unwrap();
    assert!(response.responses.is_empty());
    assert_eq!(response.throttle_time_ms, 0);
    assert!(response.node_endpoints.is_empty());
    assert!(decoder.is_empty());

    let mut decoder = Decoder::new(&[0, 0, 0, 0, 0, 0, 0, 0]);
    let response = FetchResponseV4::decode_body(&mut decoder).unwrap();
    assert!(response.responses.is_empty());
    assert_eq!(response.throttle_time_ms, 0);
    assert!(decoder.is_empty());

    let mut decoder = Decoder::new(&[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
    let response = FetchResponseV11::decode_body(&mut decoder).unwrap();
    assert!(response.responses.is_empty());
    assert_eq!(response.error_code, 0);
    assert_eq!(response.session_id, 0);
    assert!(decoder.is_empty());

    let mut decoder = Decoder::new(&[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
    let response = FetchResponseV12::decode_body(&mut decoder).unwrap();
    assert!(response.responses.is_empty());
    assert_eq!(response.error_code, 0);
    assert_eq!(response.session_id, 0);
    assert!(decoder.is_empty());
    let mut decoder = Decoder::new(&[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
    let response = FetchResponseV13::decode_body(&mut decoder).unwrap();
    assert!(response.responses.is_empty());
    assert_eq!(response.error_code, 0);
    assert_eq!(response.session_id, 0);
    assert!(decoder.is_empty());

    let mut decoder = Decoder::new(&[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
    let response = MetadataResponseV1::decode_body(&mut decoder).unwrap();
    assert!(response.brokers.is_empty());
    assert_eq!(response.controller_id, 0);
    assert!(response.topics.is_empty());
    assert!(decoder.is_empty());

    let mut decoder = Decoder::new(&[0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0]);
    let response = MetadataResponseV12::decode_body(&mut decoder).unwrap();
    assert!(response.brokers.is_empty());
    assert_eq!(response.cluster_id, None);
    assert_eq!(response.controller_id, 0);
    assert!(response.topics.is_empty());
    assert!(decoder.is_empty());

    let mut decoder = Decoder::new(&[0, 0, 0, 0]);
    let response = ListOffsetsResponseV1::decode_body(&mut decoder).unwrap();
    assert!(response.topics.is_empty());
    assert!(decoder.is_empty());

    let mut decoder = Decoder::new(&[0, 0, 0, 0, 0, 0, 0, 0]);
    let response = OffsetForLeaderEpochResponseV3::decode_body(&mut decoder).unwrap();
    assert_eq!(response.throttle_time_ms, 0);
    assert!(response.topics.is_empty());
    assert!(decoder.is_empty());

    let mut decoder = Decoder::new(&[0, 0, 0, 0, 0, 0]);
    let response = ApiVersionsResponseV0::decode_body(&mut decoder).unwrap();
    assert_eq!(response.error_code, 0);
    assert!(response.api_keys.is_empty());
    assert!(decoder.is_empty());

    let mut decoder = Decoder::new(&[0, 0, 1, 0, 0, 0, 0, 0]);
    let response = ApiVersionsResponseV3::decode_body(&mut decoder).unwrap();
    assert_eq!(response.error_code, 0);
    assert!(response.api_keys.is_empty());
    assert_eq!(response.throttle_time_ms, 0);
    assert!(response.supported_features.is_empty());
    assert!(response.finalized_features.is_empty());
    assert!(response.tagged_fields.is_empty());
    assert!(decoder.is_empty());
    let mut decoder = Decoder::new(&[0, 0, 1, 0, 0, 0, 0, 0]);
    let response = ApiVersionsResponseV4::decode_body(&mut decoder).unwrap();
    assert_eq!(response.error_code, 0);
    assert!(response.api_keys.is_empty());
    assert_eq!(response.throttle_time_ms, 0);
    assert!(response.supported_features.is_empty());
    assert!(response.finalized_features.is_empty());
    assert!(response.tagged_fields.is_empty());
    assert!(decoder.is_empty());
}
