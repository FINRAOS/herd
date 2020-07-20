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

import org.finra.herd.model.api.xml.EmrCluster;
import org.finra.herd.model.api.xml.EmrClusterCreateRequest;
import org.finra.herd.model.api.xml.EmrClusterDefinition;
import org.finra.herd.model.api.xml.EmrHadoopJarStep;
import org.finra.herd.model.api.xml.EmrHadoopJarStepAddRequest;
import org.finra.herd.model.api.xml.EmrHiveStep;
import org.finra.herd.model.api.xml.EmrHiveStepAddRequest;
import org.finra.herd.model.api.xml.EmrMasterSecurityGroup;
import org.finra.herd.model.api.xml.EmrMasterSecurityGroupAddRequest;
import org.finra.herd.model.api.xml.EmrPigStep;
import org.finra.herd.model.api.xml.EmrPigStepAddRequest;
import org.finra.herd.model.api.xml.EmrShellStep;
import org.finra.herd.model.api.xml.EmrShellStepAddRequest;
import org.finra.herd.model.dto.EmrClusterAlternateKeyDto;
import org.finra.herd.service.EmrService;

/**
 * This class tests various functionality within the EMR REST controller.
 */
public class EmrRestControllerTest extends AbstractRestTest
{
    @InjectMocks
    private EmrRestController emrRestController;

    @Mock
    private EmrService emrService;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testAddHadoopJarStepToEmrCluster() throws Exception
    {
        // Create an add step request.
        EmrHadoopJarStepAddRequest emrHadoopJarStepAddRequest =
            new EmrHadoopJarStepAddRequest(NAMESPACE, EMR_CLUSTER_DEFINITION_NAME, EMR_CLUSTER_NAME, EMR_STEP_NAME, EMR_STEP_JAR_LOCATION, EMR_STEP_MAIN_CLASS,
                Arrays.asList(ATTRIBUTE_NAME_1_MIXED_CASE), CONTINUE_ON_ERROR, EMR_CLUSTER_ID, AWS_ACCOUNT_ID);

        // Create an add step response.
        EmrHadoopJarStep emrHadoopJarStep =
            new EmrHadoopJarStep(EMR_STEP_ID, NAMESPACE, EMR_CLUSTER_DEFINITION_NAME, EMR_CLUSTER_NAME, EMR_STEP_NAME, EMR_STEP_JAR_LOCATION,
                EMR_STEP_MAIN_CLASS, Arrays.asList(ATTRIBUTE_NAME_1_MIXED_CASE), CONTINUE_ON_ERROR, EMR_CLUSTER_ID);

        // Mock the external calls.
        when(emrService.addStepToCluster(emrHadoopJarStepAddRequest)).thenReturn(emrHadoopJarStep);

        // Call the method under test.
        EmrHadoopJarStep result = emrRestController.addHadoopJarStepToEmrCluster(emrHadoopJarStepAddRequest);

        // Verify the external calls.
        verify(emrService).addStepToCluster(emrHadoopJarStepAddRequest);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(emrHadoopJarStep, result);
    }

    @Test
    public void testAddHiveStepToEmrCluster() throws Exception
    {
        // Create an add step request.
        EmrHiveStepAddRequest emrHiveStepAddRequest =
            new EmrHiveStepAddRequest(NAMESPACE, EMR_CLUSTER_DEFINITION_NAME, EMR_CLUSTER_NAME, EMR_STEP_NAME, EMR_STEP_SCRIPT_LOCATION,
                Arrays.asList(ATTRIBUTE_NAME_1_MIXED_CASE), CONTINUE_ON_ERROR, EMR_CLUSTER_ID, AWS_ACCOUNT_ID);

        // Create an add step response.
        EmrHiveStep emrHiveStep =
            new EmrHiveStep(EMR_STEP_ID, NAMESPACE, EMR_CLUSTER_DEFINITION_NAME, EMR_CLUSTER_NAME, EMR_STEP_NAME, EMR_STEP_SCRIPT_LOCATION,
                Arrays.asList(ATTRIBUTE_NAME_1_MIXED_CASE), CONTINUE_ON_ERROR, EMR_CLUSTER_ID);

        // Mock the external calls.
        when(emrService.addStepToCluster(emrHiveStepAddRequest)).thenReturn(emrHiveStep);

        // Call the method under test.
        EmrHiveStep result = emrRestController.addHiveStepToEmrCluster(emrHiveStepAddRequest);

        // Verify the external calls.
        verify(emrService).addStepToCluster(emrHiveStepAddRequest);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(emrHiveStep, result);
    }

    @Test
    public void testAddPigStepToEmrCluster() throws Exception
    {
        // Create an add step request.
        EmrPigStepAddRequest emrPigStepAddRequest =
            new EmrPigStepAddRequest(NAMESPACE, EMR_CLUSTER_DEFINITION_NAME, EMR_CLUSTER_NAME, EMR_STEP_NAME, EMR_STEP_SCRIPT_LOCATION,
                Arrays.asList(ATTRIBUTE_NAME_1_MIXED_CASE), CONTINUE_ON_ERROR, EMR_CLUSTER_ID, AWS_ACCOUNT_ID);

        // Create an add step response.
        EmrPigStep emrPigStep = new EmrPigStep(EMR_STEP_ID, NAMESPACE, EMR_CLUSTER_DEFINITION_NAME, EMR_CLUSTER_NAME, EMR_STEP_NAME, EMR_STEP_SCRIPT_LOCATION,
            Arrays.asList(ATTRIBUTE_NAME_1_MIXED_CASE), CONTINUE_ON_ERROR, EMR_CLUSTER_ID);

        // Mock the external calls.
        when(emrService.addStepToCluster(emrPigStepAddRequest)).thenReturn(emrPigStep);

        // Call the method under test.
        EmrPigStep result = emrRestController.addPigStepToEmrCluster(emrPigStepAddRequest);

        // Verify the external calls.
        verify(emrService).addStepToCluster(emrPigStepAddRequest);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(emrPigStep, result);
    }

    @Test
    public void testAddShellStepToEmrCluster() throws Exception
    {
        // Create an add step request.
        EmrShellStepAddRequest emrShellStepAddRequest =
            new EmrShellStepAddRequest(NAMESPACE, EMR_CLUSTER_DEFINITION_NAME, EMR_CLUSTER_NAME, EMR_STEP_NAME, EMR_STEP_SCRIPT_LOCATION,
                Arrays.asList(ATTRIBUTE_NAME_1_MIXED_CASE), CONTINUE_ON_ERROR, EMR_CLUSTER_ID, AWS_ACCOUNT_ID);

        // Create an add step response.
        EmrShellStep emrShellStep =
            new EmrShellStep(EMR_STEP_ID, NAMESPACE, EMR_CLUSTER_DEFINITION_NAME, EMR_CLUSTER_NAME, EMR_STEP_NAME, EMR_STEP_SCRIPT_LOCATION,
                Arrays.asList(ATTRIBUTE_NAME_1_MIXED_CASE), CONTINUE_ON_ERROR, EMR_CLUSTER_ID);

        // Mock the external calls.
        when(emrService.addStepToCluster(emrShellStepAddRequest)).thenReturn(emrShellStep);

        // Call the method under test.
        EmrShellStep result = emrRestController.addShellStepToEmrCluster(emrShellStepAddRequest);

        // Verify the external calls.
        verify(emrService).addStepToCluster(emrShellStepAddRequest);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(emrShellStep, result);
    }

    @Test
    public void testCreateEmrCluster() throws Exception
    {
        // Create an EMR cluster definition override.
        EmrClusterDefinition emrClusterDefinitionOverride = new EmrClusterDefinition();

        // Create an EMR cluster create request.
        EmrClusterCreateRequest emrClusterCreateRequest =
            new EmrClusterCreateRequest(NAMESPACE, EMR_CLUSTER_DEFINITION_NAME, EMR_CLUSTER_NAME, DRY_RUN, emrClusterDefinitionOverride);

        // Create an EMR cluster.
        EmrCluster emrCluster = new EmrCluster();
        emrCluster.setId(EMR_CLUSTER_ID);

        // Mock the external calls.
        when(emrService.createCluster(emrClusterCreateRequest)).thenReturn(emrCluster);

        // Call the method under test.
        EmrCluster result = emrRestController.createEmrCluster(emrClusterCreateRequest);

        // Verify the external calls.
        verify(emrService).createCluster(emrClusterCreateRequest);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(emrCluster, result);
    }

    @Test
    public void testGetEmrCluster() throws Exception
    {
        // Create an EMR cluster key.
        EmrClusterAlternateKeyDto emrClusterAlternateKeyDto = new EmrClusterAlternateKeyDto(NAMESPACE, EMR_CLUSTER_DEFINITION_NAME, EMR_CLUSTER_NAME);

        // Create an EMR cluster.
        EmrCluster emrCluster = new EmrCluster();
        emrCluster.setId(EMR_CLUSTER_ID);

        // Mock the external calls.
        when(emrService.getCluster(emrClusterAlternateKeyDto, EMR_CLUSTER_ID, EMR_STEP_ID, EMR_CLUSTER_VERBOSE_FLAG, AWS_ACCOUNT_ID, RETRIEVE_INSTANCE_FLEETS))
            .thenReturn(emrCluster);

        // Call the method under test.
        EmrCluster result = emrRestController
            .getEmrCluster(NAMESPACE, EMR_CLUSTER_DEFINITION_NAME, EMR_CLUSTER_NAME, EMR_CLUSTER_ID, EMR_STEP_ID, EMR_CLUSTER_VERBOSE_FLAG, AWS_ACCOUNT_ID,
                RETRIEVE_INSTANCE_FLEETS);

        // Verify the external calls.
        verify(emrService)
            .getCluster(emrClusterAlternateKeyDto, EMR_CLUSTER_ID, EMR_STEP_ID, EMR_CLUSTER_VERBOSE_FLAG, AWS_ACCOUNT_ID, RETRIEVE_INSTANCE_FLEETS);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(emrCluster, result);
    }

    @Test
    public void testTerminateEmrCluster() throws Exception
    {
        // Create an EMR cluster key.
        EmrClusterAlternateKeyDto emrClusterAlternateKeyDto = new EmrClusterAlternateKeyDto(NAMESPACE, EMR_CLUSTER_DEFINITION_NAME, EMR_CLUSTER_NAME);

        // Create an EMR cluster.
        EmrCluster emrCluster = new EmrCluster();
        emrCluster.setId(EMR_CLUSTER_ID);

        // Mock the external calls.
        when(emrService.terminateCluster(emrClusterAlternateKeyDto, OVERRIDE_TERMINATION_PROTECTION, EMR_CLUSTER_ID, AWS_ACCOUNT_ID)).thenReturn(emrCluster);

        // Call the method under test.
        EmrCluster result = emrRestController
            .terminateEmrCluster(NAMESPACE, EMR_CLUSTER_DEFINITION_NAME, EMR_CLUSTER_NAME, OVERRIDE_TERMINATION_PROTECTION, EMR_CLUSTER_ID, AWS_ACCOUNT_ID);

        // Verify the external calls.
        verify(emrService).terminateCluster(emrClusterAlternateKeyDto, OVERRIDE_TERMINATION_PROTECTION, EMR_CLUSTER_ID, AWS_ACCOUNT_ID);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(emrCluster, result);
    }

    /**
     * Checks if any of the mocks has any interaction.
     */
    private void verifyNoMoreInteractionsHelper()
    {
        verifyNoMoreInteractions(emrService);
    }
}
