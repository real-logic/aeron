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
package io.aeron.test;

public class SystemTestConfig
{
    public static final String DRIVER_AWAIT_COUNTER_CLOSE_PROP_NAME = "aeron.test.system.driver.await.counters";
    public static final boolean DRIVER_AWAIT_COUNTER_CLOSE = Boolean.getBoolean(DRIVER_AWAIT_COUNTER_CLOSE_PROP_NAME);
    public static final long MIN_COUNTER_CLOSE_INTERRUPT_TIMEOUT_MS = 20_000L;
}
