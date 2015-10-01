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

import java.io.IOException;
import java.io.StringWriter;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

/**
 * A helper class for JSON functionality.
 */
@Component
public class JsonHelper
{
    /**
     * Serializes any Java value as JSON output.
     *
     * @param object the Java object to be serialized
     *
     * @return the JSON representation of the object
     * @throws IOException if an I/O error occurred
     */
    public String objectToJson(Object object) throws IOException
    {
        ObjectMapper mapper = new ObjectMapper();

        StringWriter sw = new StringWriter();
        mapper.writeValue(sw, object);

        return sw.toString();
    }

    /**
     * Deserializes JSON content from given JSON content String.
     *
     * @param classType the class type of the object
     * @param jsonContent the JSON string
     *
     * @return the object
     * @throws java.io.IOException if there is an error in unmarshalling
     */
    public <T> T unmarshallJsonToObject(Class<T> classType, String jsonContent) throws IOException
    {
        return new ObjectMapper().readValue(jsonContent, classType);
    }
}
