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
package org.finra.herd.rest;

import static org.junit.Assert.assertNotNull;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.Test;

import org.finra.herd.model.api.xml.BuildInformation;

/**
 * This class tests various functionality within the herd REST controller.
 */
public class HerdRestControllerTest extends AbstractRestTest
{
    private static Logger logger = LoggerFactory.getLogger(HerdRestControllerTest.class);

    @Test
    public void testGetBuildInfo() throws Exception
    {
        // Get the build information and ensure it is valid.
        BuildInformation buildInformation = herdRestController.getBuildInfo();
        assertNotNull(buildInformation);
        assertNotNull(buildInformation.getBuildDate());
        logger.info(buildInformation.toString());
    }

    @Test
    public void testValidateNoDuplicateQueryStringParams() throws Exception
    {
        // Add a key with a single value which is allowed.
        Map<String, String[]> parameterMap = new HashMap<>();
        String[] singleValue = new String[1];
        singleValue[0] = "testValue"; // Single Value
        parameterMap.put("testKey1", singleValue);

        // Add a key with 2 values which which isn't normally allowed, but is not a problem because we aren't looking for it in the validate method below.
        String[] multipleValues = new String[2];
        multipleValues[0] = "testValue1";
        multipleValues[1] = "testValue2";
        parameterMap.put("testKey2", multipleValues);

        // Validate the query string parameters, but only for "testKey1" and not "testKey2".
        herdRestController.validateNoDuplicateQueryStringParams(parameterMap, "testKey1");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateNoDuplicateQueryStringParamsWithException() throws Exception
    {
        Map<String, String[]> parameterMap = new HashMap<>();
        String[] values = new String[2];
        values[0] = "testValue1"; // Duplicate Values which aren't allowed.
        values[1] = "testValue2";
        parameterMap.put("testKey", values);
        herdRestController.validateNoDuplicateQueryStringParams(parameterMap, "testKey");
    }
}
