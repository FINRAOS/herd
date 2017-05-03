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
package org.finra.herd.service.helper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.model.api.xml.GlobalAttributeDefinitionKey;
import org.finra.herd.service.AbstractServiceTest;

/**
 * This class tests functionality within the helper for global attribute definition related operations.
 */
public class GlobalAttributeDefinitionHelperTest extends AbstractServiceTest
{
    @Mock
    private AlternateKeyHelper alternateKeyHelper;

    @InjectMocks
    private GlobalAttributeDefinitionHelper globalAttributeDefinitionHelper;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testValidateGlobalAttributeDefinitionKey()
    {
        // Create a global attribute definition key.
        GlobalAttributeDefinitionKey globalAttributeDefinitionKey =
            new GlobalAttributeDefinitionKey(GLOBAL_ATTRIBUTE_DEFINITON_LEVEL, GLOBAL_ATTRIBUTE_DEFINITON_NAME);

        // Mock the external calls.
        when(alternateKeyHelper.validateStringParameter("global attribute definition level", GLOBAL_ATTRIBUTE_DEFINITON_LEVEL))
            .thenReturn(GLOBAL_ATTRIBUTE_DEFINITON_LEVEL);
        when(alternateKeyHelper.validateStringParameter("global attribute definition name", GLOBAL_ATTRIBUTE_DEFINITON_NAME))
            .thenReturn(GLOBAL_ATTRIBUTE_DEFINITON_NAME);

        // Call the method under test.
        globalAttributeDefinitionHelper.validateGlobalAttributeDefinitionKey(globalAttributeDefinitionKey);

        // Verify the external calls.
        verify(alternateKeyHelper).validateStringParameter("global attribute definition level", GLOBAL_ATTRIBUTE_DEFINITON_LEVEL);
        verify(alternateKeyHelper).validateStringParameter("global attribute definition name", GLOBAL_ATTRIBUTE_DEFINITON_NAME);
        verifyNoMoreInteractions(alternateKeyHelper);

        // Validate the results.
        assertEquals(new GlobalAttributeDefinitionKey(GLOBAL_ATTRIBUTE_DEFINITON_LEVEL, GLOBAL_ATTRIBUTE_DEFINITON_NAME), globalAttributeDefinitionKey);
    }

    @Test
    public void testValidateGlobalAttributeDefinitionKeyMissingRequiredParameters()
    {
        // Try to call the method under test.
        try
        {
            globalAttributeDefinitionHelper.validateGlobalAttributeDefinitionKey(null);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A global attribute definition key must be specified.", e.getMessage());
        }

        // Verify the external calls.
        verifyNoMoreInteractions(alternateKeyHelper);
    }
}
