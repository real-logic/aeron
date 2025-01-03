/*
 * Copyright 2014-2025 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.cluster;

class ClusterTestConstants
{
    static final String INGRESS_ENDPOINT = "localhost:20000";
    static final String INGRESS_ENDPOINTS = "0=" + INGRESS_ENDPOINT;
    static final String CLUSTER_MEMBERS =
        "0," + INGRESS_ENDPOINT + ",localhost:20001,localhost:20002,localhost:0,localhost:8010";
    static final long CATALOG_CAPACITY = 1024 * 1024;
}
