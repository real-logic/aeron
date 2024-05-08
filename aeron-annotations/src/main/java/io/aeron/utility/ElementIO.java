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
package io.aeron.utility;

import io.aeron.config.ConfigInfo;
import io.aeron.counter.CounterInfo;

import javax.tools.FileObject;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.OutputStream;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 */
@SuppressWarnings("checkstyle:MissingJavadocMethod")
public class ElementIO
{
    @SuppressWarnings("unchecked")
    public static <T> List<T> fetch(final String elementsFilename) throws Exception
    {
        return ((ElementList<T>)acquireContext()
            .createUnmarshaller()
            .unmarshal(Paths.get(elementsFilename).toFile())).element;
    }

    public static <T> void write(final FileObject resourceFile, final Map<String, T> elements) throws Exception
    {
        final Marshaller marshaller = acquireContext().createMarshaller();
        marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);

        try (OutputStream out = resourceFile.openOutputStream())
        {
            marshaller.marshal(new ElementList<>(elements), out);
        }
    }

    private static JAXBContext acquireContext() throws JAXBException
    {
        return JAXBContext.newInstance(ElementList.class, ConfigInfo.class, CounterInfo.class);
    }

    /**
     * @param <T>
     */
    @XmlRootElement
    public static class ElementList<T>
    {
        public List<T> element;

        /**
         */
        public ElementList()
        {
        }

        /**
         * @param elementMap
         */
        public ElementList(final Map<String, T> elementMap)
        {
            this.element = new ArrayList<>(elementMap.values());
        }
    }
}
