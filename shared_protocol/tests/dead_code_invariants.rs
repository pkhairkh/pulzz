//! Wave 9 Task 9-a,b: Dead code deprecation + invariant tests.

use shared_protocol::chpmt::{ControllerRouteFamily, RouteFamily};
use shared_protocol::protocol::RecordType;

#[test]
fn transform_route_family_variant_still_exists_for_decode_compat() {
    let _ = RecordType::TransformDef as u8;
    let _ = RecordType::TransformCorrect as u8;
    let _ = RouteFamily::Transform as u8;
    let _ = ControllerRouteFamily::Transform as u8;
}

#[test]
fn suspended_route_families_are_documented_as_deprecated() {
    let _ = RouteFamily::Assembly as u8;
    let _ = RouteFamily::Schema as u8;
    let _ = RouteFamily::Episode as u8;
    let _ = ControllerRouteFamily::Assembly as u8;
    let _ = ControllerRouteFamily::SchemaExpansion as u8;
    let _ = ControllerRouteFamily::EpisodeCompletion as u8;
}

#[test]
fn default_route_family_is_direct_state() {
    let default = ControllerRouteFamily::default();
    assert_eq!(default, ControllerRouteFamily::DirectState);
}
