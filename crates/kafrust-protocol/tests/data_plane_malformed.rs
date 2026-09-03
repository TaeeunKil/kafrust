use kafrust_protocol::api::api_versions::{ApiVersionsResponseV0, ApiVersionsResponseV3};
use kafrust_protocol::api::fetch::{FetchResponseV12, FetchResponseV4};
use kafrust_protocol::api::list_offsets::ListOffsetsResponseV1;
use kafrust_protocol::api::metadata::{MetadataResponseV1, MetadataResponseV12};
use kafrust_protocol::api::offset_for_leader_epoch::OffsetForLeaderEpochResponseV3;
use kafrust_protocol::api::produce::{ProduceResponseV13, ProduceResponseV2, ProduceResponseV9};
use kafrust_protocol::codec::Decoder;

#[test]
fn rejects_truncated_data_plane_response_families() {
    assert!(ProduceResponseV2::decode_body(&mut Decoder::new(&[0, 0, 0])).is_err());
    assert!(FetchResponseV4::decode_body(&mut Decoder::new(&[0, 0, 0])).is_err());
    assert!(ListOffsetsResponseV1::decode_body(&mut Decoder::new(&[0, 0, 0])).is_err());
    assert!(MetadataResponseV1::decode_body(&mut Decoder::new(&[0, 0, 0])).is_err());
    assert!(ApiVersionsResponseV0::decode_body(&mut Decoder::new(&[0, 0, 0])).is_err());
    assert!(OffsetForLeaderEpochResponseV3::decode_body(&mut Decoder::new(&[0, 0, 0])).is_err());
}

#[test]
fn rejects_negative_or_truncated_collection_lengths() {
    let negative_array = [(-2_i32).to_be_bytes(), 0_i32.to_be_bytes()]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    assert!(ProduceResponseV2::decode_body(&mut Decoder::new(&negative_array)).is_err());
    let negative_fetch_array = [0_i32.to_be_bytes(), (-2_i32).to_be_bytes()]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    assert!(FetchResponseV4::decode_body(&mut Decoder::new(&negative_fetch_array)).is_err());
    assert!(ListOffsetsResponseV1::decode_body(&mut Decoder::new(&negative_array)).is_err());

    let truncated_flexible = [1_u8, 0, 0, 0];
    assert!(MetadataResponseV12::decode_body(&mut Decoder::new(&truncated_flexible)).is_err());
    assert!(ApiVersionsResponseV3::decode_body(&mut Decoder::new(&truncated_flexible)).is_err());
    assert!(ProduceResponseV9::decode_body(&mut Decoder::new(&truncated_flexible)).is_err());
}

#[test]
fn rejects_truncated_flexible_tag_sections() {
    // Each body declares one top-level tagged field but omits its tag ID and
    // payload. The decoder must fail before returning a partially parsed body.
    assert!(ProduceResponseV9::decode_body(&mut Decoder::new(&[1, 0, 0, 0, 0, 1])).is_err());
    assert!(ProduceResponseV13::decode_body(&mut Decoder::new(&[1, 0, 0, 0, 0, 1])).is_err());
    assert!(FetchResponseV12::decode_body(&mut Decoder::new(&[
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1
    ]))
    .is_err());
    assert!(MetadataResponseV12::decode_body(&mut Decoder::new(&[
        0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 1
    ]))
    .is_err());
    assert!(
        ApiVersionsResponseV3::decode_body(&mut Decoder::new(&[0, 0, 1, 0, 0, 0, 0, 1])).is_err()
    );
}
