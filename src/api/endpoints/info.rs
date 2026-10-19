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

#[derive(serde::Serialize, utoipa::ToSchema)]
pub struct InfoResponse {
    pub service_version: &'static str,
    pub api_version: &'static str,
}

#[utoipa::path(
    get,
    path = "/api/info",
    tag = "health",
    responses(
        (status = 200, description = "Service version information", body = InfoResponse)
    )
)]
pub async fn info() -> axum::Json<InfoResponse> {
    axum::Json(InfoResponse {
        service_version: env!("CARGO_PKG_VERSION"),
        api_version: "v1",
    })
}
