/*
 * Copyright 2014-2024 Real Logic Limited.
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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * This has some disabled tests as they are evaluating behaviour of the callback when tests fail, so enabling them
 * will cause the full build to fail. The tests are there, so that the code can be easily tested manually.
 */
@ExtendWith(ThreadNamingTestCallback.class)
public class ThreadNamingCallbackTest
{
    private String threadName = "unset";

    @BeforeEach
    void setUp()
    {
        threadName = Thread.currentThread().getName();
        assertThat(threadName, not(containsString("TEST")));
    }

    @Test
    void testThatSucceeds()
    {
        assertThat(Thread.currentThread().getName(), containsString("testThatSucceeds"));
    }

    @Test
    @Disabled
    void testThatFails()
    {
        assertThat(Thread.currentThread().getName(), containsString(this.getClass().getSimpleName()));
        assertThat(Thread.currentThread().getName(), containsString("testThatFails"));
        fail("forced failure");
    }

    @Test
    @Disabled
    void testThatThrowsCheckedException() throws Exception
    {
        assertThat(Thread.currentThread().getName(), containsString(this.getClass().getSimpleName()));
        assertThat(Thread.currentThread().getName(), containsString("testThatThrowsCheckedException"));
        throw new Exception("forced failure");
    }

    @Test
    @Disabled
    void testThatThrowsUncheckedException()
    {
        assertThat(Thread.currentThread().getName(), containsString(this.getClass().getSimpleName()));
        assertThat(Thread.currentThread().getName(), containsString("testThatThrowsUncheckedException"));
        throw new RuntimeException("forced failure");
    }

    @AfterEach
    void tearDown()
    {
        assertNotEquals(threadName, Thread.currentThread().getName());
    }
}
