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

use crate::openapi_helpers::{
    CompatibilityRequestDoc, CompatibilityResponseDoc, InteractionSessionMessageDoc,
    PostRunRequestDoc,
};

#[utoipa::path(
    post,
    path = "/api/executor/run",
    tag = "crossflow",
    request_body = PostRunRequestDoc,
    responses(
        (status = 200, description = "Workflow result; shape is determined by the diagram's terminal node"),
        (status = 422, description = "Unprocessable — diagram or request is invalid"),
        (status = 500, description = "Internal server error"),
    )
)]
#[allow(dead_code)]
pub async fn run() {}

#[utoipa::path(
    post,
    path = "/api/executor/compatibility",
    tag = "crossflow",
    request_body = CompatibilityRequestDoc,
    responses(
        (status = 200, description = "Compatibility results for each requested connection", body = CompatibilityResponseDoc),
        (status = 500, description = "Internal server error"),
    )
)]
#[allow(dead_code)]
pub async fn compatibility() {}

#[utoipa::path(
    get,
    path = "/api/executor/interaction",
    tag = "crossflow",
    responses(
        (status = 101, description = "Upgrades to WebSocket; client sends a PostRunRequest JSON text frame to start the workflow; server sends InteractionSessionMessage JSON text frames (feedback events and a final finish frame)", body = InteractionSessionMessageDoc),
    )
)]
#[allow(dead_code)]
pub async fn interaction() {}
