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

import org.junit.ClassRule;
import org.junit.Test;
import uk.co.real_logic.aeron.mediadriver.TemplateFileResource;

import java.io.IOException;

import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static uk.co.real_logic.aeron.mediadriver.TemplateFileResource.BUFFER_SIZE;

public class SenderChannelBufferTest
{
    @ClassRule
    public static TemplateFileResource templateResource = new TemplateFileResource();

    @Test
    public void buffersAreRotatedAroundWithIndexMapping() throws IOException
    {
        final SenderChannelBuffer buffer = new SenderChannelBuffer(templateResource.templateFile(), templateResource.directory(), BUFFER_SIZE);
        for (long termId = 0; termId < 20; termId++)
        {
            assertThat(buffer.newTermBuffer(termId), notNullValue());
            assertThat(buffer.get(termId), notNullValue());

            if (termId > 3)
            {
                long oldTermId = termId - 3;
                assertThat(buffer.get(oldTermId), nullValue());
            }
        }
    }

}
