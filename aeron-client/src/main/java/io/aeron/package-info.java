/*
 * Copyright 2014-2018 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * {@link io.aeron.Aeron} client that is used to communicate to a local Media Driver for the publication and
 * subscription to message streams.
 * <p>
 * A client can be created by invoking {@link io.aeron.Aeron#connect()} for default setting or via
 * {@link io.aeron.Aeron#connect(io.aeron.Aeron.Context)} to override the defaults or system properties.
 */
package io.aeron;