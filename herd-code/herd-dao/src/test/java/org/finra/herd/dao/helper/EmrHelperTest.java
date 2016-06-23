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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.elasticmapreduce.model.Cluster;
import com.amazonaws.services.elasticmapreduce.model.ClusterState;
import com.amazonaws.services.elasticmapreduce.model.ClusterStatus;
import com.amazonaws.services.elasticmapreduce.model.ClusterSummary;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.oozie.client.WorkflowAction;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import org.finra.herd.dao.AbstractDaoTest;
import org.finra.herd.dao.EmrDao;
import org.finra.herd.dao.impl.MockOozieWorkflowAction;
import org.finra.herd.dao.impl.MockOozieWorkflowJob;
import org.finra.herd.dao.impl.OozieDaoImpl;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.EmrClusterAlternateKeyDto;

/**
 * This class tests functionality within the AwsHelper class.
 */
public class EmrHelperTest extends AbstractDaoTest
{
    @Autowired
    EmrHelper emrHelper;

    @Test
    public void testBuildEmrClusterName() throws Exception
    {
        String clusterName = emrHelper.buildEmrClusterName(NAMESPACE, EMR_CLUSTER_DEFINITION_NAME, EMR_CLUSTER_NAME);

        assertEquals(NAMESPACE + "." + EMR_CLUSTER_DEFINITION_NAME + "." + EMR_CLUSTER_NAME, clusterName);
    }

    @Test
    public void testValidateEmrClusterKey() throws Exception
    {
        EmrClusterAlternateKeyDto emrClusterAlternateKeyDto = new EmrClusterAlternateKeyDto();
        emrClusterAlternateKeyDto.setNamespace(NAMESPACE + " ");
        emrClusterAlternateKeyDto.setEmrClusterDefinitionName(EMR_CLUSTER_DEFINITION_NAME + " ");
        emrClusterAlternateKeyDto.setEmrClusterName(EMR_CLUSTER_NAME + " ");


        emrHelper.validateEmrClusterKey(emrClusterAlternateKeyDto);

        // Ensure values are trimmed
        assertEquals(NAMESPACE, emrClusterAlternateKeyDto.getNamespace());
        assertEquals(EMR_CLUSTER_DEFINITION_NAME, emrClusterAlternateKeyDto.getEmrClusterDefinitionName());
        assertEquals(EMR_CLUSTER_NAME, emrClusterAlternateKeyDto.getEmrClusterName());
    }

    @Test
    public void testGetS3StagingLocation() throws Exception
    {
        String s3StagingLocation = emrHelper.getS3StagingLocation();

        assertNotNull("s3 staging location is null", s3StagingLocation);
    }

    @Test
    public void testGetS3HdfsCopyScriptName() throws Exception
    {
        String s3HdfsCopyScriptName = emrHelper.getS3HdfsCopyScriptName();

        assertNotNull("s3 staging location is null", s3HdfsCopyScriptName);
    }

    @Test
    public void testGetS3HdfsCopyScriptNameMissing() throws Exception
    {
        removeReloadablePropertySourceFromEnvironment();
        try
        {
            emrHelper.getS3HdfsCopyScriptName();

            fail("Expected a IllegalStateException, but not exception was thrown");
        }
        catch (Exception e)
        {
            assertEquals(IllegalStateException.class, e.getClass());
        }
        finally
        {
            restorePropertySourceInEnvironment();
        }
    }

    @Test
    public void testEmrOozieHerdWorkflowS3LocationConfiguration() throws Exception
    {
        ConfigurationValue s3HdfsCopyScriptName = emrHelper.getEmrOozieHerdWorkflowS3LocationConfiguration();

        assertNotNull("s3 staging location is null", s3HdfsCopyScriptName);
    }

    @Test
    public void testEmrOozieHerdWorkflowS3LocationConfigurationMissing() throws Exception
    {
        removeReloadablePropertySourceFromEnvironment();
        try
        {
            emrHelper.getS3HdfsCopyScriptName();

            fail("Expected a IllegalStateException, but not exception was thrown");
        }
        catch (Exception e)
        {
            assertEquals(IllegalStateException.class, e.getClass());
        }
        finally
        {
            restorePropertySourceInEnvironment();
        }
    }

    @Test
    public void testClientWorkflowAction() throws Exception
    {
        MockOozieWorkflowJob wrapperWorkflowJob = new MockOozieWorkflowJob();
        List<WorkflowAction> actions = new ArrayList<>();

        MockOozieWorkflowAction action = new MockOozieWorkflowAction();
        action.setName(OozieDaoImpl.ACTION_NAME_CLIENT_WORKFLOW);
        actions.add(action);

        wrapperWorkflowJob.setActions(actions);

        WorkflowAction clientAction = emrHelper.getClientWorkflowAction(wrapperWorkflowJob);

        assertNotNull("no client workflow action found", clientAction);
    }

    @Test
    public void testClientWorkflowActionNotFound() throws Exception
    {
        MockOozieWorkflowJob wrapperWorkflowJob = new MockOozieWorkflowJob();
        WorkflowAction wrapperAction = emrHelper.getClientWorkflowAction(wrapperWorkflowJob);

        assertNull("client workflow action found", wrapperAction);
    }

    @Test
    public void testFirstWorkflowActionInError() throws Exception
    {
        MockOozieWorkflowJob wrapperWorkflowJob = new MockOozieWorkflowJob();
        List<WorkflowAction> actions = new ArrayList<>();

        MockOozieWorkflowAction action = new MockOozieWorkflowAction();
        action.setStatus(WorkflowAction.Status.ERROR);
        actions.add(action);

        wrapperWorkflowJob.setActions(actions);

        WorkflowAction clientAction = emrHelper.getFirstWorkflowActionInError(wrapperWorkflowJob);

        assertNotNull("no error action found", clientAction);
    }

    @Test
    public void testFirstWorkflowActionInErrorNotFound() throws Exception
    {
        MockOozieWorkflowJob wrapperWorkflowJob = new MockOozieWorkflowJob();
        WorkflowAction wrapperAction = emrHelper.getClientWorkflowAction(wrapperWorkflowJob);

        assertNull("error action found", wrapperAction);
    }

    @Test
    public void testIsActiveEmrState() throws Exception
    {
        boolean isActive = emrHelper.isActiveEmrState("RUNNING");

        assertTrue("not active", isActive);
    }

    @Test
    public void testEmrHadoopJarStepConfig() throws Exception
    {
        StepConfig stepConfig = emrHelper.getEmrHadoopJarStepConfig("step_name", "jar_location", null, null, false);

        assertNotNull("step not retuned", stepConfig);

        assertEquals("name not found", "step_name", stepConfig.getName());
        assertEquals("jar not found", "jar_location", stepConfig.getHadoopJarStep().getJar());
    }

    @Test
    public void testEmrHadoopJarStepConfigNoContinueOnError() throws Exception
    {
        StepConfig stepConfig = emrHelper.getEmrHadoopJarStepConfig("step_name", "jar_location", null, null, null);

        assertNotNull("step not retuned", stepConfig);

        assertEquals("name not found", "step_name", stepConfig.getName());
        assertEquals("jar not found", "jar_location", stepConfig.getHadoopJarStep().getJar());
    }

    @Test
    public void testEmrHadoopJarStepConfigContinueOnError() throws Exception
    {
        StepConfig stepConfig = emrHelper.getEmrHadoopJarStepConfig("step_name", "jar_location", null, null, true);

        assertNotNull("step not retuned", stepConfig);

        assertEquals("name not found", "step_name", stepConfig.getName());
        assertEquals("jar not found", "jar_location", stepConfig.getHadoopJarStep().getJar());
    }

    @Test
    public void testEmrHadoopJarStepConfigWithArguments() throws Exception
    {
        List<String> arguments = new ArrayList<String>();
        arguments.add("arg1");

        StepConfig stepConfig = emrHelper.getEmrHadoopJarStepConfig("step_name", "jar_location", null, arguments, false);

        assertNotNull("step not retuned", stepConfig);

        assertEquals("name not found", "step_name", stepConfig.getName());
        assertEquals("jar not found", "jar_location", stepConfig.getHadoopJarStep().getJar());
        assertNotNull("arguments not found", stepConfig.getHadoopJarStep().getArgs());
    }

    @Test
    public void testGetActiveEmrClusterIdAssertReturnActualClusterIdWhenClusterIdSpecifiedAndClusterStateActiveAndNameMatch()
    {
        EmrDao originalEmrDao = emrHelper.getEmrDao();
        EmrDao mockEmrDao = mock(EmrDao.class);
        emrHelper.setEmrDao(mockEmrDao);

        try
        {
            String emrClusterId = "emrClusterId";
            String emrClusterName = "emrClusterName";
            String expectedEmrClusterId = "expectedEmrClusterId";

            when(mockEmrDao.getEmrClusterById(any(), any())).thenReturn(
                new Cluster().withId(expectedEmrClusterId).withName(emrClusterName).withStatus(new ClusterStatus().withState(ClusterState.RUNNING)));

            assertEquals(expectedEmrClusterId, emrHelper.getActiveEmrClusterId(emrClusterId, emrClusterName));

            verify(mockEmrDao).getEmrClusterById(eq(emrClusterId.trim()), any());
            verifyNoMoreInteractions(mockEmrDao);
        }
        finally
        {
            emrHelper.setEmrDao(originalEmrDao);
        }
    }

    @Test
    public void testGetActiveEmrClusterIdAssertErrorWhenClusterIdSpecifiedAndNameMismatch()
    {
        EmrDao originalEmrDao = emrHelper.getEmrDao();
        EmrDao mockEmrDao = mock(EmrDao.class);
        emrHelper.setEmrDao(mockEmrDao);

        try
        {
            String emrClusterId = "emrClusterId";
            String emrClusterName = "emrClusterName";
            String expectedEmrClusterId = "expectedEmrClusterId";
            String actualEmrClusterName = "actualEmrClusterName";

            when(mockEmrDao.getEmrClusterById(any(), any())).thenReturn(
                new Cluster().withId(expectedEmrClusterId).withName(actualEmrClusterName).withStatus(new ClusterStatus().withState(ClusterState.RUNNING)));

            try
            {
                emrHelper.getActiveEmrClusterId(emrClusterId, emrClusterName);
                fail();
            }
            catch (IllegalArgumentException e)
            {
                assertEquals(String
                    .format("The cluster with ID \"%s\" does not match the expected name \"%s\". The actual name is \"%s\".", expectedEmrClusterId,
                        emrClusterName, actualEmrClusterName), e.getMessage());
            }

            verify(mockEmrDao).getEmrClusterById(eq(emrClusterId.trim()), any());
            verifyNoMoreInteractions(mockEmrDao);
        }
        finally
        {
            emrHelper.setEmrDao(originalEmrDao);
        }
    }

    @Test
    public void testGetActiveEmrClusterIdAssertReturnActualClusterIdWhenClusterStateActiveAndNameNotSpecified()
    {
        EmrDao originalEmrDao = emrHelper.getEmrDao();
        EmrDao mockEmrDao = mock(EmrDao.class);
        emrHelper.setEmrDao(mockEmrDao);

        try
        {
            String emrClusterId = "emrClusterId";
            String emrClusterName = null;
            String expectedEmrClusterId = "expectedEmrClusterId";
            String actualEmrClusterName = "actualEmrClusterName";

            when(mockEmrDao.getEmrClusterById(any(), any())).thenReturn(
                new Cluster().withId(expectedEmrClusterId).withName(actualEmrClusterName).withStatus(new ClusterStatus().withState(ClusterState.RUNNING)));

            assertEquals(expectedEmrClusterId, emrHelper.getActiveEmrClusterId(emrClusterId, emrClusterName));

            verify(mockEmrDao).getEmrClusterById(eq(emrClusterId), any());
            verifyNoMoreInteractions(mockEmrDao);
        }
        finally
        {
            emrHelper.setEmrDao(originalEmrDao);
        }
    }

    @Test
    public void testGetActiveEmrClusterIdAssertErrorWhenClusterIdSpecifiedAndClusterStateNotActive()
    {
        EmrDao originalEmrDao = emrHelper.getEmrDao();
        EmrDao mockEmrDao = mock(EmrDao.class);
        emrHelper.setEmrDao(mockEmrDao);

        try
        {
            String emrClusterId = "emrClusterId";
            String emrClusterName = "emrClusterName";
            String expectedEmrClusterId = "expectedEmrClusterId";

            ClusterState actualClusterState = ClusterState.TERMINATED;
            when(mockEmrDao.getEmrClusterById(any(), any()))
                .thenReturn(new Cluster().withId(expectedEmrClusterId).withName(emrClusterName).withStatus(new ClusterStatus().withState(actualClusterState)));

            try
            {
                emrHelper.getActiveEmrClusterId(emrClusterId, emrClusterName);
                fail();
            }
            catch (IllegalArgumentException e)
            {
                assertEquals(String.format("The cluster with ID \"%s\" is not active. The cluster state must be in one of [STARTING, BOOTSTRAPPING, RUNNING, " +
                    "WAITING]. Current state is \"%s\"", emrClusterId, actualClusterState), e.getMessage());
            }

            verify(mockEmrDao).getEmrClusterById(eq(emrClusterId), any());
            verifyNoMoreInteractions(mockEmrDao);
        }
        finally
        {
            emrHelper.setEmrDao(originalEmrDao);
        }
    }

    @Test
    public void testGetActiveEmrClusterIdAssertErrorWhenClusterIdSpecifiedAndClusterDoesNotExist()
    {
        EmrDao originalEmrDao = emrHelper.getEmrDao();
        EmrDao mockEmrDao = mock(EmrDao.class);
        emrHelper.setEmrDao(mockEmrDao);

        try
        {
            String emrClusterId = "emrClusterId";
            String emrClusterName = "emrClusterName";

            when(mockEmrDao.getEmrClusterById(any(), any())).thenReturn(null);

            try
            {
                emrHelper.getActiveEmrClusterId(emrClusterId, emrClusterName);
                fail();
            }
            catch (IllegalArgumentException e)
            {
                assertEquals(String.format("The cluster with ID \"%s\" does not exist.", emrClusterId), e.getMessage());
            }

            verify(mockEmrDao).getEmrClusterById(eq(emrClusterId), any());
            verifyNoMoreInteractions(mockEmrDao);
        }
        finally
        {
            emrHelper.setEmrDao(originalEmrDao);
        }
    }

    @Test
    public void testGetActiveEmrClusterIdAssertParametersTrimmed()
    {
        EmrDao originalEmrDao = emrHelper.getEmrDao();
        EmrDao mockEmrDao = mock(EmrDao.class);
        emrHelper.setEmrDao(mockEmrDao);

        try
        {
            String emrClusterId = "emrClusterId";
            String emrClusterName = "emrClusterName";
            String expectedEmrClusterId = "expectedEmrClusterId";

            when(mockEmrDao.getEmrClusterById(any(), any())).thenReturn(
                new Cluster().withId(expectedEmrClusterId).withName(emrClusterName).withStatus(new ClusterStatus().withState(ClusterState.RUNNING)));

            assertEquals(expectedEmrClusterId,
                emrHelper.getActiveEmrClusterId(StringUtils.wrap(emrClusterId, BLANK_TEXT), StringUtils.wrap(emrClusterName, BLANK_TEXT)));

            verify(mockEmrDao).getEmrClusterById(eq(emrClusterId.trim()), any());
            verifyNoMoreInteractions(mockEmrDao);
        }
        finally
        {
            emrHelper.setEmrDao(originalEmrDao);
        }
    }

    @Test
    public void testGetActiveEmrClusterIdAssertParametersCaseIgnored()
    {
        EmrDao originalEmrDao = emrHelper.getEmrDao();
        EmrDao mockEmrDao = mock(EmrDao.class);
        emrHelper.setEmrDao(mockEmrDao);

        try
        {
            String emrClusterId = "emrClusterId";
            String emrClusterName = "emrClusterName";
            String expectedEmrClusterId = "expectedEmrClusterId";

            when(mockEmrDao.getEmrClusterById(any(), any())).thenReturn(
                new Cluster().withId(expectedEmrClusterId).withName(emrClusterName).withStatus(new ClusterStatus().withState(ClusterState.RUNNING)));

            assertEquals(expectedEmrClusterId, emrHelper.getActiveEmrClusterId(StringUtils.upperCase(emrClusterId), StringUtils.upperCase(emrClusterName)));

            verify(mockEmrDao).getEmrClusterById(eq(StringUtils.upperCase(emrClusterId)), any());
            verifyNoMoreInteractions(mockEmrDao);
        }
        finally
        {
            emrHelper.setEmrDao(originalEmrDao);
        }
    }

    @Test
    public void testGetActiveEmrClusterIdNoIdSpecifiedAssertReturnActualClusterId()
    {
        EmrDao originalEmrDao = emrHelper.getEmrDao();
        EmrDao mockEmrDao = mock(EmrDao.class);
        emrHelper.setEmrDao(mockEmrDao);

        try
        {
            String emrClusterId = null;
            String emrClusterName = "emrClusterName";
            String expectedEmrClusterId = "expectedEmrClusterId";

            when(mockEmrDao.getActiveEmrClusterByName(any(), any())).thenReturn(new ClusterSummary().withId(expectedEmrClusterId).withName(emrClusterName));

            assertEquals(expectedEmrClusterId, emrHelper.getActiveEmrClusterId(emrClusterId, emrClusterName));

            verify(mockEmrDao).getActiveEmrClusterByName(eq(emrClusterName), any());
            verifyNoMoreInteractions(mockEmrDao);
        }
        finally
        {
            emrHelper.setEmrDao(originalEmrDao);
        }
    }

    @Test
    public void testGetActiveEmrClusterIdNoIdSpecifiedAssertErrorWhenClusterDoesNotExist()
    {
        EmrDao originalEmrDao = emrHelper.getEmrDao();
        EmrDao mockEmrDao = mock(EmrDao.class);
        emrHelper.setEmrDao(mockEmrDao);

        try
        {
            String emrClusterId = null;
            String emrClusterName = "emrClusterName";

            when(mockEmrDao.getActiveEmrClusterByName(any(), any())).thenReturn(null);

            try
            {
                emrHelper.getActiveEmrClusterId(emrClusterId, emrClusterName);
                fail();
            }
            catch (IllegalArgumentException e)
            {
                assertEquals(String.format("The cluster with name \"%s\" does not exist.", emrClusterName), e.getMessage());
            }

            verify(mockEmrDao).getActiveEmrClusterByName(eq(emrClusterName), any());
            verifyNoMoreInteractions(mockEmrDao);
        }
        finally
        {
            emrHelper.setEmrDao(originalEmrDao);
        }
    }

    @Test
    public void testGetActiveEmrClusterIdAssertErrorWhenBothIdAndNameNotSpecified()
    {
        EmrDao originalEmrDao = emrHelper.getEmrDao();
        EmrDao mockEmrDao = mock(EmrDao.class);
        emrHelper.setEmrDao(mockEmrDao);

        try
        {
            String emrClusterId = null;
            String emrClusterName = null;

            try
            {
                emrHelper.getActiveEmrClusterId(emrClusterId, emrClusterName);
                fail();
            }
            catch (IllegalArgumentException e)
            {
                assertEquals("One of EMR cluster ID or EMR cluster name must be specified.", e.getMessage());
            }

            verifyNoMoreInteractions(mockEmrDao);
        }
        finally
        {
            emrHelper.setEmrDao(originalEmrDao);
        }
    }
}

