/*
 * Copyright (C) 2026 ROS-Industrial Consortium Asia Pacific
 * Advanced Remanufacturing and Technology Centre
 * A*STAR Research Entities (Co. Registration No. 199702110H)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use utoipa::openapi::RefOr;

/// Minimal newtype wrapper: gives utoipa the correct schema name for $ref
/// generation in operation annotations.  The actual schema content is
/// injected into the served OpenAPI JSON by `build_openapi_json` via schemars.
macro_rules! bridge_schema {
    ($wrapper:ident, $name:literal) => {
        pub struct $wrapper;

        impl utoipa::PartialSchema for $wrapper {
            fn schema() -> RefOr<utoipa::openapi::schema::Schema> {
                RefOr::T(utoipa::openapi::schema::Schema::Object(
                    utoipa::openapi::schema::ObjectBuilder::new().build(),
                ))
            }
        }

        impl utoipa::ToSchema for $wrapper {
            fn name() -> std::borrow::Cow<'static, str> {
                std::borrow::Cow::Borrowed($name)
            }
        }
    };
}

bridge_schema!(PostRunRequestDoc, "PostRunRequest");
bridge_schema!(DiagramElementMetadataDoc, "DiagramElementMetadata");
bridge_schema!(CompatibilityRequestDoc, "CompatibilityRequest");
bridge_schema!(CompatibilityResponseDoc, "CompatibilityResponse");
bridge_schema!(InteractionSessionMessageDoc, "InteractionSessionMessage");

// ── OpenAPI JSON patching ────────────────────────────────────────────────────

/// Rewrites `#/$defs/Foo` refs to `#/components/schemas/Foo`, converts
/// boolean schemas to their object equivalents, and strips JSON Schema
/// 2020-12 meta-keywords that OpenAPI tooling doesn't understand.
fn normalize_schema(v: &mut serde_json::Value) {
    match v {
        serde_json::Value::Bool(true) => *v = serde_json::json!({}),
        serde_json::Value::Bool(false) => *v = serde_json::json!({"not": {}}),
        serde_json::Value::Object(map) => {
            if let Some(r) = map.get_mut("$ref")
                && let Some(s) = r.as_str()
                && let Some(name) = s.strip_prefix("#/$defs/")
            {
                *r = serde_json::Value::String(format!("#/components/schemas/{name}"));
            }
            for key in &[
                "$schema",
                "$id",
                "$anchor",
                "$dynamicAnchor",
                "$dynamicRef",
                "$vocabulary",
                "$comment",
            ] {
                map.remove(*key);
            }
            for val in map.values_mut() {
                normalize_schema(val);
            }
        }
        serde_json::Value::Array(arr) => {
            for val in arr.iter_mut() {
                normalize_schema(val);
            }
        }
        _ => {}
    }
}

/// Recursively extracts all `$defs` entries into `out` (normalizing as it
/// goes), so every nested sub-schema ends up as a top-level component.
fn collect_defs(json: &serde_json::Value, out: &mut serde_json::Map<String, serde_json::Value>) {
    if let Some(defs) = json
        .as_object()
        .and_then(|o| o.get("$defs"))
        .and_then(|d| d.as_object())
    {
        for (name, schema) in defs {
            if out.contains_key(name) {
                continue;
            }
            let mut s = schema.clone();
            normalize_schema(&mut s);
            collect_defs(&s.clone(), out);
            out.insert(name.clone(), s);
        }
    }
}

fn inject_schemars_schemas(schemas: &mut serde_json::Map<String, serde_json::Value>) {
    let roots: &[(&str, serde_json::Value)] = &[
        (
            "PostRunRequest",
            serde_json::json!({
                "type": "object",
                "required": ["diagram", "request"],
                "properties": {
                    "diagram": { "$ref": "#/components/schemas/Diagram" },
                    "request": {}
                }
            }),
        ),
        (
            "Diagram",
            serde_json::to_value(schemars::schema_for!(crossflow::Diagram)).unwrap_or_default(),
        ),
        (
            "DiagramElementMetadata",
            serde_json::to_value(schemars::schema_for!(crossflow::DiagramElementMetadata))
                .unwrap_or_default(),
        ),
        (
            "CompatibilityRequest",
            serde_json::to_value(schemars::schema_for!(
                crossflow_diagram_editor::api::executor::CompatibilityRequest
            ))
            .unwrap_or_default(),
        ),
        (
            "CompatibilityResponse",
            serde_json::to_value(schemars::schema_for!(
                crossflow_diagram_editor::api::executor::CompatibilityResponse
            ))
            .unwrap_or_default(),
        ),
        (
            "InteractionSessionMessage",
            serde_json::to_value(schemars::schema_for!(
                crossflow_diagram_editor::api::executor::InteractionSessionMessage
            ))
            .unwrap_or_default(),
        ),
    ];

    for (root_name, raw) in roots {
        let mut json = raw.clone();
        normalize_schema(&mut json);

        let mut defs = serde_json::Map::new();
        collect_defs(&json, &mut defs);
        schemas.extend(defs);

        if let Some(obj) = json.as_object_mut() {
            obj.remove("$defs");
        }
        schemas.insert(root_name.to_string(), json);
    }
}

/// Builds the OpenAPI JSON spec: starts from utoipa's path/operation metadata
/// then injects real schemars-generated schemas into `components.schemas`.
pub fn build_openapi_json(base: utoipa::openapi::OpenApi) -> serde_json::Value {
    let mut json = serde_json::to_value(base).unwrap_or_default();

    let mut extra = serde_json::Map::new();
    inject_schemars_schemas(&mut extra);

    if let Some(obj) = json.as_object_mut() {
        let schemas = obj
            .entry("components")
            .or_insert_with(|| serde_json::json!({}))
            .as_object_mut()
            .unwrap()
            .entry("schemas")
            .or_insert_with(|| serde_json::json!({}))
            .as_object_mut()
            .unwrap();
        schemas.extend(extra);
    }

    json
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_schema_rewrites_defs_ref() {
        let mut v = serde_json::json!({"$ref": "#/$defs/Foo"});
        normalize_schema(&mut v);
        assert_eq!(v["$ref"], "#/components/schemas/Foo");
    }

    #[test]
    fn normalize_schema_leaves_components_ref_untouched() {
        let mut v = serde_json::json!({"$ref": "#/components/schemas/Bar"});
        normalize_schema(&mut v);
        assert_eq!(v["$ref"], "#/components/schemas/Bar");
    }

    #[test]
    fn normalize_schema_strips_meta_keywords() {
        let mut v = serde_json::json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$id": "urn:example",
            "$anchor": "root",
            "type": "object"
        });
        normalize_schema(&mut v);
        assert!(v.get("$schema").is_none());
        assert!(v.get("$id").is_none());
        assert!(v.get("$anchor").is_none());
        assert_eq!(v["type"], "object");
    }

    #[test]
    fn normalize_schema_converts_bool_true() {
        let mut v = serde_json::Value::Bool(true);
        normalize_schema(&mut v);
        assert_eq!(v, serde_json::json!({}));
    }

    #[test]
    fn normalize_schema_converts_bool_false() {
        let mut v = serde_json::Value::Bool(false);
        normalize_schema(&mut v);
        assert_eq!(v, serde_json::json!({"not": {}}));
    }

    #[test]
    fn normalize_schema_recurses_into_nested_objects() {
        let mut v = serde_json::json!({
            "properties": {
                "child": {"$ref": "#/$defs/Child", "$schema": "drop-me"}
            }
        });
        normalize_schema(&mut v);
        assert_eq!(
            v["properties"]["child"]["$ref"],
            "#/components/schemas/Child"
        );
        assert!(v["properties"]["child"].get("$schema").is_none());
    }

    #[test]
    fn collect_defs_extracts_top_level_defs() {
        let json = serde_json::json!({
            "$defs": {
                "Foo": {"type": "string"},
                "Bar": {"type": "integer"}
            }
        });
        let mut out = serde_json::Map::new();
        collect_defs(&json, &mut out);
        assert!(out.contains_key("Foo"));
        assert!(out.contains_key("Bar"));
    }

    #[test]
    fn collect_defs_does_not_overwrite_existing_entry() {
        let json = serde_json::json!({"$defs": {"Foo": {"type": "string"}}});
        let mut out = serde_json::Map::new();
        out.insert("Foo".into(), serde_json::json!({"type": "number"}));
        collect_defs(&json, &mut out);
        assert_eq!(out["Foo"]["type"], "number");
    }

    #[test]
    fn build_openapi_json_injects_schemars_schemas() {
        use utoipa::OpenApi;

        #[derive(OpenApi)]
        #[openapi()]
        struct MinimalDoc;

        let result = build_openapi_json(MinimalDoc::openapi());
        let schemas = &result["components"]["schemas"];
        assert!(schemas.is_object(), "components.schemas must be an object");
        assert!(
            schemas.get("PostRunRequest").is_some(),
            "PostRunRequest schema must be present"
        );
        assert!(
            schemas.get("DiagramElementMetadata").is_some(),
            "DiagramElementMetadata schema must be present"
        );
        assert!(
            schemas.get("CompatibilityRequest").is_some(),
            "CompatibilityRequest schema must be present"
        );
        assert!(
            schemas.get("CompatibilityResponse").is_some(),
            "CompatibilityResponse schema must be present"
        );
        assert!(
            schemas.get("CompatibilityConnection").is_some(),
            "CompatibilityConnection schema must be present"
        );
        assert!(
            schemas.get("CompatibilityResult").is_some(),
            "CompatibilityResult schema must be present"
        );
        assert!(
            schemas.get("CompatibilityStatus").is_some(),
            "CompatibilityStatus schema must be present"
        );
        assert!(
            schemas.get("InteractionSessionMessage").is_some(),
            "InteractionSessionMessage schema must be present"
        );
        assert!(
            schemas.get("PortRef").is_some(),
            "PortRef schema must be present"
        );
    }

    #[test]
    fn build_openapi_json_merges_with_existing_schemas() {
        use utoipa::OpenApi;

        #[derive(utoipa::ToSchema)]
        struct MyType {
            _x: i32,
        }

        #[derive(OpenApi)]
        #[openapi(components(schemas(MyType)))]
        struct DocWithSchema;

        let result = build_openapi_json(DocWithSchema::openapi());
        let schemas = &result["components"]["schemas"];
        assert!(
            schemas.get("MyType").is_some(),
            "utoipa schema must be kept"
        );
        assert!(
            schemas.get("PostRunRequest").is_some(),
            "injected schema must also be present"
        );
    }
}
