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
package org.finra.herd.dao.helper;

import java.io.IOException;
import java.io.StringWriter;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.json.simple.JSONObject;
import org.springframework.stereotype.Component;

/**
 * A helper class for JSON functionality.
 */
@Component
public class JsonHelper
{
    /**
     * Gets key value from the specified JSON object. The method throws an exception, when specified key does not exist.
     *
     * @param jsonObject the JSON object that contains the key value
     * @param key the key name
     * @param classType the class to cast the result to
     * @param <T> the return type
     *
     * @return the key value
     */
    public <T> T getKeyValue(JSONObject jsonObject, Object key, Class<T> classType)
    {
        Object result = jsonObject.get(key);

        if (result == null)
        {
            throw new IllegalArgumentException(String.format("Failed to get \"%s\" key value from JSON object.", key.toString()));
        }

        try
        {
            return classType.cast(result);
        }
        catch (ClassCastException e)
        {
            throw new IllegalArgumentException(String.format("Failed to cast \"%s\" key value to %s.", result.toString(), classType.getName()), e);
        }
    }

    /**
     * Serializes any Java value as JSON output.
     *
     * @param object the Java object to be serialized
     *
     * @return the JSON representation of the object
     * @throws IllegalStateException when an I/O error occurs
     */
    public String objectToJson(Object object) throws IllegalStateException
    {
        ObjectMapper mapper = new ObjectMapper();

        StringWriter stringWriter = new StringWriter();

        try
        {
            mapper.writeValue(stringWriter, object);
        }
        catch (IOException e)
        {
            throw new IllegalStateException(e);
        }

        return stringWriter.toString();
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
