/*
* Copyright 2015 herd contributors
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.finra.dm.dao.helper;

import java.io.StringWriter;
import java.nio.charset.StandardCharsets;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

import org.apache.commons.io.IOUtils;
import org.eclipse.persistence.jaxb.MarshallerProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * A helper class for XML functionality.
 */
@Component
public class XmlHelper
{
    @Autowired
    private DmCharacterEscapeHandler dmCharacterEscapeHandler;

    /**
     * Returns XML representation of the object.
     *
     * @param obj the Java object to be serialized
     *
     * @return the XML representation of this object
     * @throws javax.xml.bind.JAXBException if a JAXB error occurred.
     */
    public String objectToXml(Object obj) throws JAXBException
    {
        // By default, we do not ask the marshalled XML data to be formatted with line feeds and indentation.
        return objectToXml(obj, false);
    }

    /**
     * Returns XML representation of the object using the DM custom character escape handler.
     *
     * @param obj the Java object to be serialized
     * @param formatted specifies whether or not the marshalled XML data is formatted with line feeds and indentation
     *
     * @return the XML representation of this object
     * @throws JAXBException if a JAXB error occurred.
     */
    public String objectToXml(Object obj, boolean formatted) throws JAXBException
    {
        JAXBContext requestContext = JAXBContext.newInstance(obj.getClass());
        Marshaller requestMarshaller = requestContext.createMarshaller();

        if (formatted)
        {
            requestMarshaller.setProperty(Marshaller.JAXB_ENCODING, StandardCharsets.UTF_8.name());
            requestMarshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
        }

        // Specify a custom character escape handler to escape XML 1.1 restricted characters.
        requestMarshaller.setProperty(MarshallerProperties.CHARACTER_ESCAPE_HANDLER, dmCharacterEscapeHandler);

        StringWriter sw = new StringWriter();
        requestMarshaller.marshal(obj, sw);

        return sw.toString();
    }

    /**
     * Unmarshalls the xml into JAXB object.
     *
     * @param classType the class type of JAXB element
     * @param xmlString the xml string
     * @param <T> the class type.
     *
     * @return the JAXB object
     * @throws javax.xml.bind.JAXBException if there is an error in unmarshalling
     */
    @SuppressWarnings("unchecked")
    public <T> T unmarshallXmlToObject(Class<T> classType, String xmlString) throws JAXBException
    {
        JAXBContext context = JAXBContext.newInstance(classType);
        Unmarshaller un = context.createUnmarshaller();
        return (T) un.unmarshal(IOUtils.toInputStream(xmlString));
    }
}
