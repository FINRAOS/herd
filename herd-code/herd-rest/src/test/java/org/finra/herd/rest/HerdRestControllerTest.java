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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;
import java.util.Date;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.model.api.xml.BuildInformation;
import org.finra.herd.model.api.xml.TimeoutValidationResponse;

/**
 * This class tests various functionality within the herd REST controller.
 */
public class HerdRestControllerTest extends AbstractRestTest
{
    @Mock
    private BuildInformation buildInformation;

    @InjectMocks
    private HerdRestController herdRestController;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testGetBuildInfo()
    {
        // Call the method under test.
        BuildInformation result = herdRestController.getBuildInfo();

        // Validate the results.
        assertEquals(buildInformation, result);
    }

    @Test
    public void testGetTimeoutValidation() throws InterruptedException
    {
        Integer testSeconds = 1;
        Date start = new Date();
        // Call the method under test.
        TimeoutValidationResponse result = herdRestController.getTimeoutValidation(testSeconds);
        long timeDiff = (new Date().getTime() - start.getTime()) / 1000;
        assertTrue(  timeDiff >= testSeconds);
        // Validate the results.
        assertEquals("Successfully waited for " + testSeconds + " seconds.", result.getMessage());
    }

    @Test
    public void testGetTimeoutValidationNegativeNumber() throws InterruptedException {
        try
        {
            herdRestController.getTimeoutValidation(-1);
            fail("Expected an IllegalArgumentException to be thrown");
        } catch (IllegalArgumentException e) {
            assertEquals("Specified value \"-1\" does not fall within the range of 0 to 1800 seconds.", e.getMessage());
        }
    }

    @Test
    public void testGetTimeoutValidationTooHighNumber() throws InterruptedException {
        try
        {
            herdRestController.getTimeoutValidation(1900);
            fail("Expected an IllegalArgumentException to be thrown");
        } catch (IllegalArgumentException e) {
            assertEquals("Specified value \"1900\" does not fall within the range of 0 to 1800 seconds.", e.getMessage());
        }
    }

    @Test
    public void testGetTimeoutValidationNullParameter() throws InterruptedException {
        try
        {
            herdRestController.getTimeoutValidation(null);
            fail("Expected an IllegalArgumentException to be thrown");
        } catch (IllegalArgumentException e) {
            assertEquals("waitForSeconds query parameter is required.", e.getMessage());
        }
    }

    @Test
    public void testValidateNoDuplicateQueryStringParams()
    {
        // Create a map of parameters.
        Map<String, String[]> parameters = new HashMap<>();

        // Add a key with a single value which is allowed.
        parameters.put(ATTRIBUTE_NAME_1_MIXED_CASE, new String[] {ATTRIBUTE_VALUE_1});

        // Add a key with two values which isn't allowed.
        parameters.put(ATTRIBUTE_NAME_2_MIXED_CASE, new String[] {ATTRIBUTE_VALUE_2, ATTRIBUTE_VALUE_3});

        // Validate the query string parameters for the first key.
        herdRestController.validateNoDuplicateQueryStringParams(parameters, ATTRIBUTE_NAME_1_MIXED_CASE);

        // Try to validate the query string parameters for the second key that has multiple values.
        try
        {
            herdRestController.validateNoDuplicateQueryStringParams(parameters, ATTRIBUTE_NAME_2_MIXED_CASE);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String
                .format("Found 2 occurrences of query string parameter \"%s\", but 1 expected. Values found: \"%s, %s\".", ATTRIBUTE_NAME_2_MIXED_CASE,
                    ATTRIBUTE_VALUE_2, ATTRIBUTE_VALUE_3), e.getMessage());
        }
    }
}
