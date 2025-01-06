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
/**
 * Basic security infrastructure for authenticating sessions by delegating to a supplied instance of an
 * {@link io.aeron.security.Authenticator}.
 * <p>
 * New connection requests will be allocated a unique 64-bit session identity which is first passed to
 * {@link io.aeron.security.Authenticator#onConnectRequest(long, byte[], long)}. Once the response stream to the client
 * is connected then the system will make periodic calls to
 * {@link io.aeron.security.Authenticator#onConnectedSession(io.aeron.security.SessionProxy, long)} until the
 * {@link io.aeron.security.Authenticator} updates the status of the {@link io.aeron.security.SessionProxy} to indicate
 * if the session is authenticated, needs to be challenged, or is rejected.
 */
package io.aeron.security;