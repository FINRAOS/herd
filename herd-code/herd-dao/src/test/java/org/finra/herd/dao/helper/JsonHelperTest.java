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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.math.BigDecimal;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import org.finra.herd.dao.AbstractDaoTest;

/**
 * This class tests functionality within the JsonHelper class.
 */
public class JsonHelperTest extends AbstractDaoTest
{
    @Autowired
    private JsonHelper jsonHelper;

    @Test
    public void testGetKeyValue() throws Exception
    {
        // Create a JSON object with one key value pair.
        JSONParser jsonParser = new JSONParser();
        JSONObject jsonObject = (JSONObject) jsonParser.parse(String.format("{\"%s\" : \"%s\"}", KEY, VALUE));

        // Get and validate the key value.
        assertEquals(VALUE, jsonHelper.getKeyValue(jsonObject, KEY, String.class));

        // Try to get a value for a non-existing key.
        try
        {
            jsonHelper.getKeyValue(jsonObject, I_DO_NOT_EXIST, String.class);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Failed to get \"%s\" key value from JSON object.", I_DO_NOT_EXIST), e.getMessage());
        }

        // Try to get a value that cannot be cast to the specified class type.
        try
        {
            jsonHelper.getKeyValue(jsonObject, KEY, BigDecimal.class);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Failed to cast \"%s\" key value to %s.", VALUE, BigDecimal.class.getName()), e.getMessage());
        }
    }

    @Test
    public void testObjectToJson()
    {
        assertEquals(String.format("\"%s\"", STRING_VALUE), jsonHelper.objectToJson(STRING_VALUE));
    }

    @Test
    public void testUnmarshallJsonToObject() throws IOException
    {
        assertEquals(STRING_VALUE, jsonHelper.unmarshallJsonToObject(String.class, String.format("\"%s\"", STRING_VALUE)));
    }
}
