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

import org.finra.herd.model.api.xml.EmrClusterDefinition;
import org.finra.herd.model.api.xml.EmrClusterDefinitionCreateRequest;
import org.finra.herd.model.api.xml.EmrClusterDefinitionInformation;
import org.finra.herd.model.api.xml.EmrClusterDefinitionKey;
import org.finra.herd.model.api.xml.EmrClusterDefinitionKeys;
import org.finra.herd.model.api.xml.EmrClusterDefinitionUpdateRequest;
import org.finra.herd.service.EmrClusterDefinitionService;

/**
 * This class tests various functionality within the EMR Cluster Definition REST controller.
 */
public class EmrClusterDefinitionRestControllerTest extends AbstractRestTest
{
    @InjectMocks
    private EmrClusterDefinitionRestController emrClusterDefinitionRestController;

    @Mock
    private EmrClusterDefinitionService emrClusterDefinitionService;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCreateEmrClusterDefinition() throws Exception
    {
        // Create an EMR cluster definition key.
        EmrClusterDefinitionKey emrClusterDefinitionKey = new EmrClusterDefinitionKey(NAMESPACE, EMR_CLUSTER_DEFINITION_NAME);

        // Create an EMR cluster definition.
        EmrClusterDefinition emrClusterDefinition = new EmrClusterDefinition();

        // Create an EMR cluster definition create request.
        EmrClusterDefinitionCreateRequest emrClusterDefinitionCreateRequest =
            new EmrClusterDefinitionCreateRequest(emrClusterDefinitionKey, emrClusterDefinition);

        // Create an object that holds EMR cluster definition information.
        EmrClusterDefinitionInformation emrClusterDefinitionInformation =
            new EmrClusterDefinitionInformation(ID, emrClusterDefinitionKey, emrClusterDefinition);

        // Mock the external calls.
        when(emrClusterDefinitionService.createEmrClusterDefinition(emrClusterDefinitionCreateRequest)).thenReturn(emrClusterDefinitionInformation);

        // Call the method under test.
        EmrClusterDefinitionInformation result = emrClusterDefinitionRestController.createEmrClusterDefinition(emrClusterDefinitionCreateRequest);

        // Verify the external calls.
        verify(emrClusterDefinitionService).createEmrClusterDefinition(emrClusterDefinitionCreateRequest);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(emrClusterDefinitionInformation, result);
    }

    @Test
    public void testDeleteEmrClusterDefinition() throws Exception
    {
        // Create an EMR cluster definition key.
        EmrClusterDefinitionKey emrClusterDefinitionKey = new EmrClusterDefinitionKey(NAMESPACE, EMR_CLUSTER_DEFINITION_NAME);

        // Create an EMR cluster definition.
        EmrClusterDefinition emrClusterDefinition = new EmrClusterDefinition();

        // Create an object that holds EMR cluster definition information.
        EmrClusterDefinitionInformation emrClusterDefinitionInformation =
            new EmrClusterDefinitionInformation(ID, emrClusterDefinitionKey, emrClusterDefinition);

        // Mock the external calls.
        when(emrClusterDefinitionService.deleteEmrClusterDefinition(emrClusterDefinitionKey)).thenReturn(emrClusterDefinitionInformation);

        // Call the method under test.
        EmrClusterDefinitionInformation result = emrClusterDefinitionRestController.deleteEmrClusterDefinition(NAMESPACE, EMR_CLUSTER_DEFINITION_NAME);

        // Verify the external calls.
        verify(emrClusterDefinitionService).deleteEmrClusterDefinition(emrClusterDefinitionKey);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(emrClusterDefinitionInformation, result);
    }

    @Test
    public void testGetEmrClusterDefinition() throws Exception
    {
        // Create an EMR cluster definition key.
        EmrClusterDefinitionKey emrClusterDefinitionKey = new EmrClusterDefinitionKey(NAMESPACE, EMR_CLUSTER_DEFINITION_NAME);

        // Create an EMR cluster definition.
        EmrClusterDefinition emrClusterDefinition = new EmrClusterDefinition();

        // Create an object that holds EMR cluster definition information.
        EmrClusterDefinitionInformation emrClusterDefinitionInformation =
            new EmrClusterDefinitionInformation(ID, emrClusterDefinitionKey, emrClusterDefinition);

        // Mock the external calls.
        when(emrClusterDefinitionService.getEmrClusterDefinition(emrClusterDefinitionKey)).thenReturn(emrClusterDefinitionInformation);

        // Call the method under test.
        EmrClusterDefinitionInformation result = emrClusterDefinitionRestController.getEmrClusterDefinition(NAMESPACE, EMR_CLUSTER_DEFINITION_NAME);

        // Verify the external calls.
        verify(emrClusterDefinitionService).getEmrClusterDefinition(emrClusterDefinitionKey);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(emrClusterDefinitionInformation, result);
    }

    @Test
    public void testGetEmrClusterDefinitions() throws Exception
    {
        // Create an EMR cluster definition keys.
        EmrClusterDefinitionKeys emrClusterDefinitionKeys =
            new EmrClusterDefinitionKeys(Arrays.asList(new EmrClusterDefinitionKey(NAMESPACE, EMR_CLUSTER_DEFINITION_NAME)));

        // Mock the external calls.
        when(emrClusterDefinitionService.getEmrClusterDefinitions(NAMESPACE)).thenReturn(emrClusterDefinitionKeys);

        // Call the method under test.
        EmrClusterDefinitionKeys result = emrClusterDefinitionRestController.getEmrClusterDefinitions(NAMESPACE);

        // Verify the external calls.
        verify(emrClusterDefinitionService).getEmrClusterDefinitions(NAMESPACE);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(emrClusterDefinitionKeys, result);
    }

    @Test
    public void testUpdateEmrClusterDefinition() throws Exception
    {
        // Create an EMR cluster definition key.
        EmrClusterDefinitionKey emrClusterDefinitionKey = new EmrClusterDefinitionKey(NAMESPACE, EMR_CLUSTER_DEFINITION_NAME);

        // Create an EMR cluster definition.
        EmrClusterDefinition emrClusterDefinition = new EmrClusterDefinition();

        // Create an EMR cluster definition update request.
        EmrClusterDefinitionUpdateRequest emrClusterDefinitionUpdateRequest = new EmrClusterDefinitionUpdateRequest(emrClusterDefinition);

        // Create an object that holds EMR cluster definition information.
        EmrClusterDefinitionInformation emrClusterDefinitionInformation =
            new EmrClusterDefinitionInformation(ID, emrClusterDefinitionKey, emrClusterDefinition);

        // Mock the external calls.
        when(emrClusterDefinitionService.updateEmrClusterDefinition(emrClusterDefinitionKey, emrClusterDefinitionUpdateRequest))
            .thenReturn(emrClusterDefinitionInformation);

        // Call the method under test.
        EmrClusterDefinitionInformation result =
            emrClusterDefinitionRestController.updateEmrClusterDefinition(NAMESPACE, EMR_CLUSTER_DEFINITION_NAME, emrClusterDefinitionUpdateRequest);

        // Verify the external calls.
        verify(emrClusterDefinitionService).updateEmrClusterDefinition(emrClusterDefinitionKey, emrClusterDefinitionUpdateRequest);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(emrClusterDefinitionInformation, result);
    }

    /**
     * Checks if any of the mocks has any interaction.
     */
    private void verifyNoMoreInteractionsHelper()
    {
        verifyNoMoreInteractions(emrClusterDefinitionService);
    }
}
