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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.model.api.xml.GlobalAttributeDefinition;
import org.finra.herd.model.api.xml.GlobalAttributeDefinitionCreateRequest;
import org.finra.herd.model.api.xml.GlobalAttributeDefinitionKey;
import org.finra.herd.model.api.xml.GlobalAttributeDefinitionKeys;
import org.finra.herd.service.GlobalAttributeDefinitionService;

/**
 * This class tests the functionality of global attribute definition rest controller.
 */
public class GlobalAttributeDefinitionRestControllerTest extends AbstractRestTest
{
    @Mock
    private GlobalAttributeDefinitionService globalAttributeDefinitionService;

    @InjectMocks
    private GlobalAttributeDefinitionRestController globalAttributeDefinitionRestController;

    @Before()
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCreateGlobalAttributeDefinition()
    {
        // Create a global attribute definition key.
        GlobalAttributeDefinitionKey globalAttributeDefinitionKey =
            new GlobalAttributeDefinitionKey(GLOBAL_ATTRIBUTE_DEFINITON_LEVEL, GLOBAL_ATTRIBUTE_DEFINITON_NAME);

        // Create a global attribute definition create request.
        GlobalAttributeDefinitionCreateRequest request = new GlobalAttributeDefinitionCreateRequest(globalAttributeDefinitionKey);

        // Create a global attribute definition.
        GlobalAttributeDefinition globalAttributeDefinition = new GlobalAttributeDefinition();

        // Mock calls to external methods.
        when(globalAttributeDefinitionService.createGlobalAttributeDefinition(request)).thenReturn(globalAttributeDefinition);

        // Call the method under  test.
        GlobalAttributeDefinition response = globalAttributeDefinitionRestController.createGlobalAttributeDefinition(request);

        // Verify the external calls.
        verify(globalAttributeDefinitionService).createGlobalAttributeDefinition(request);
        verifyNoMoreInteractions(globalAttributeDefinitionService);

        // Validate the response.
        assertEquals(globalAttributeDefinition, response);
    }

    @Test
    public void testDeleteGlobalAttributeDefinition()
    {
        // Create a global attribute definition key.
        GlobalAttributeDefinitionKey globalAttributeDefinitionKey =
            new GlobalAttributeDefinitionKey(GLOBAL_ATTRIBUTE_DEFINITON_LEVEL, GLOBAL_ATTRIBUTE_DEFINITON_NAME);

        // Create a global attribute definition.
        GlobalAttributeDefinition globalAttributeDefinition = new GlobalAttributeDefinition(INTEGER_VALUE, globalAttributeDefinitionKey);

        // Mock calls to external methods.
        when(globalAttributeDefinitionService.deleteGlobalAttributeDefinition(globalAttributeDefinitionKey)).thenReturn(globalAttributeDefinition);

        // Call the method under test.
        GlobalAttributeDefinition response =
            globalAttributeDefinitionRestController.deleteGlobalAttributeDefinition(GLOBAL_ATTRIBUTE_DEFINITON_LEVEL, GLOBAL_ATTRIBUTE_DEFINITON_NAME);

        // Verify the external calls.
        verify(globalAttributeDefinitionService).deleteGlobalAttributeDefinition(globalAttributeDefinitionKey);
        verifyNoMoreInteractions(globalAttributeDefinitionService);

        // Validate the response.
        assertEquals(globalAttributeDefinition, response);
    }

    @Test
    public void testGetGlobalAttributeDefinitions()
    {
        // Create a list of global attribute definition keys.
        GlobalAttributeDefinitionKeys globalAttributeDefinitionKeys = new GlobalAttributeDefinitionKeys(Arrays
            .asList(new GlobalAttributeDefinitionKey(GLOBAL_ATTRIBUTE_DEFINITON_LEVEL, GLOBAL_ATTRIBUTE_DEFINITON_NAME),
                new GlobalAttributeDefinitionKey(GLOBAL_ATTRIBUTE_DEFINITON_LEVEL, GLOBAL_ATTRIBUTE_DEFINITON_NAME_2)));

        // Mock calls to external methods.
        when(globalAttributeDefinitionService.getGlobalAttributeDefinitionKeys()).thenReturn(globalAttributeDefinitionKeys);

        // Call the method under test.
        GlobalAttributeDefinitionKeys response = globalAttributeDefinitionRestController.getGlobalAttributeDefinitions();

        // Verify the external calls.
        verify(globalAttributeDefinitionService).getGlobalAttributeDefinitionKeys();
        verifyNoMoreInteractions(globalAttributeDefinitionService);

        // Validate the response.
        assertEquals(new GlobalAttributeDefinitionKeys(Arrays
            .asList(new GlobalAttributeDefinitionKey(GLOBAL_ATTRIBUTE_DEFINITON_LEVEL, GLOBAL_ATTRIBUTE_DEFINITON_NAME),
                new GlobalAttributeDefinitionKey(GLOBAL_ATTRIBUTE_DEFINITON_LEVEL, GLOBAL_ATTRIBUTE_DEFINITON_NAME_2))), response);
    }
}
