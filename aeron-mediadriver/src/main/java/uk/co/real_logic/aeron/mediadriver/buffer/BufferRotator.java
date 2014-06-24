/*
 * Copyright 2014 Real Logic Ltd.
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
package uk.co.real_logic.aeron.mediadriver.buffer;

import uk.co.real_logic.aeron.util.command.NewBufferMessageFlyweight;

import java.io.IOException;
import java.util.stream.Stream;

/**
 * Handles rotating buffers within a {@link uk.co.real_logic.aeron.mediadriver.Publication}
 */
public interface BufferRotator
{
    Stream<? extends LogBuffers> buffers();

    void rotate() throws IOException;

    void bufferInformation(final NewBufferMessageFlyweight newBufferMessage); // TODO: what does this do?
}
