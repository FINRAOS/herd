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
package org.finra.dm.dao;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.amazonaws.services.elasticmapreduce.model.ClusterSummary;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.Instance;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import org.finra.dm.dao.helper.AwsHelper;
import org.finra.dm.dao.helper.XmlHelper;
import org.finra.dm.model.dto.AwsParamsDto;
import org.finra.dm.model.api.xml.EmrClusterDefinition;
import org.finra.dm.model.api.xml.EmrClusterDefinitionApplication;
import org.finra.dm.model.api.xml.EmrClusterDefinitionConfiguration;
import org.finra.dm.model.api.xml.Parameter;

/**
 * This class tests functionality within the EmrDao.
 */
public class EmrDaoTest extends AbstractDaoTest
{
    @Autowired
    private AwsHelper awsHelper;

    @Autowired
    private XmlHelper xmlHelper;

    @After
    public void cleanUp()
    {
        // Terminate the test cluster if one is active.
        if (emrDao.getActiveEmrClusterByName(EMR_CLUSTER_NAME, awsHelper.getAwsParamsDto()) != null)
        {
            emrDao.terminateEmrCluster(EMR_CLUSTER_NAME, true, awsHelper.getAwsParamsDto());
        }
    }

    @Test
    public void testAddEmrStep() throws Exception
    {
        // Create a test cluster.
        emrDao.createEmrCluster(EMR_CLUSTER_NAME, getTestEmrClusterDefinition(), awsHelper.getAwsParamsDto());

        // Add an EMR Step.
        StepConfig testEmrStepConfig = new StepConfig();
        testEmrStepConfig.setName(EMR_STEP_NAME);
        testEmrStepConfig.setHadoopJarStep(new HadoopJarStepConfig());
        String stepId = emrDao.addEmrStep(EMR_CLUSTER_NAME, testEmrStepConfig, awsHelper.getAwsParamsDto());
        assertTrue(StringUtils.isNotBlank(stepId));
    }

    @Test
    public void testAddEmrMasterSecurityGroups() throws Exception
    {
        // Create a test cluster.
        emrDao.createEmrCluster(EMR_CLUSTER_NAME, getTestEmrClusterDefinition(), awsHelper.getAwsParamsDto());

        // Add security groups to the master node of the test EMR cluster.
        List<String> testSecurityGroups = Arrays.asList(EC2_SECURITY_GROUP_1, EC2_SECURITY_GROUP_2);
        List<String> resultSecurityGroups = emrDao.addEmrMasterSecurityGroups(EMR_CLUSTER_NAME, testSecurityGroups, awsHelper.getAwsParamsDto());

        // Validate the results.
        assertNotNull(resultSecurityGroups);
        assertEquals(testSecurityGroups, resultSecurityGroups);
    }

    @Test
    public void testGetEmrMasterInstance() throws Exception
    {
        // Create a test cluster.
        emrDao.createEmrCluster(EMR_CLUSTER_NAME, getTestEmrClusterDefinition(), awsHelper.getAwsParamsDto());

        // Get the master instance of the test EMR cluster.
        Instance resultMasterInstance = emrDao.getEmrMasterInstance(EMR_CLUSTER_NAME, awsHelper.getAwsParamsDto());

        // Validate the results.
        assertNotNull(resultMasterInstance);
    }

    @Test
    public void testCreateEmrCluster() throws Exception
    {
        // Check cluster summary before creation.
        assertNull(emrDao.getActiveEmrClusterByName(EMR_CLUSTER_NAME, awsHelper.getAwsParamsDto()));

        // Create a test cluster.
        assertNotNull(emrDao.createEmrCluster(EMR_CLUSTER_NAME, getTestEmrClusterDefinition(), awsHelper.getAwsParamsDto()));

        // Check the test cluster summary after creation.
        assertNotNull(emrDao.getActiveEmrClusterByName(EMR_CLUSTER_NAME, awsHelper.getAwsParamsDto()));
    }

    @Test
    public void testCreateEmrClusterUsing400Configuration() throws Exception
    {
        String clusterName = EMR_CLUSTER_NAME;
        EmrClusterDefinition emrClusterDefinition = getTestEmrClusterDefinition();
        AwsParamsDto awsParams = awsHelper.getAwsParamsDto();

        emrClusterDefinition.setReleaseLabel("emr-4.0.0");

        List<EmrClusterDefinitionApplication> applications = new ArrayList<>();
        {
            EmrClusterDefinitionApplication application = new EmrClusterDefinitionApplication();
            application.setAdditionalInfoList(Arrays.asList(new Parameter("testParameterName", "testParameterValue")));
            application.setArgs(Arrays.asList("arg1", "arg2"));
            application.setName("testName1");
            application.setVersion("testVersion1");
            applications.add(application);
        }
        {
            EmrClusterDefinitionApplication application = new EmrClusterDefinitionApplication();
            application.setName("testName2");
            application.setVersion("testVersion2");
            applications.add(application);
        }
        emrClusterDefinition.setApplications(applications);

        List<EmrClusterDefinitionConfiguration> configurations = new ArrayList<>();
        {
            EmrClusterDefinitionConfiguration configuration = new EmrClusterDefinitionConfiguration();
            configuration.setClassification("testClassification");
            configuration.setConfigurations(Arrays.asList(new EmrClusterDefinitionConfiguration("testChildClassification", null, null)));
            configuration.setProperties(Arrays.asList(new Parameter("testParameterName", "testParameterValue")));
            configurations.add(configuration);
        }
        {
            EmrClusterDefinitionConfiguration configuration = new EmrClusterDefinitionConfiguration();
            configuration.setClassification("testClassification");
            configurations.add(configuration);
        }
        emrClusterDefinition.setConfigurations(configurations);

        // Check cluster summary before creation.
        assertNull(emrDao.getActiveEmrClusterByName(clusterName, awsParams));

        // Create a test cluster.
        assertNotNull(emrDao.createEmrCluster(clusterName, emrClusterDefinition, awsParams));

        // Check the test cluster summary after creation.
        assertNotNull(emrDao.getActiveEmrClusterByName(clusterName, awsParams));
    }

    @Test
    public void testTerminateEmrCluster() throws Exception
    {
        // Create a test cluster.
        emrDao.createEmrCluster(EMR_CLUSTER_NAME, getTestEmrClusterDefinition(), awsHelper.getAwsParamsDto());

        // Check the test cluster summary after creation.
        assertNotNull(emrDao.getActiveEmrClusterByName(EMR_CLUSTER_NAME, awsHelper.getAwsParamsDto()));

        // Terminate the test cluster.
        emrDao.terminateEmrCluster(EMR_CLUSTER_NAME, true, awsHelper.getAwsParamsDto());

        // Check the test cluster status after termination.
        assertNull(emrDao.getActiveEmrClusterByName(EMR_CLUSTER_NAME, awsHelper.getAwsParamsDto()));
    }

    @Test
    public void testGetActiveEmrClusterByName() throws Exception
    {
        // Check cluster summary before creation.
        ClusterSummary clusterSummary = emrDao.getActiveEmrClusterByName(EMR_CLUSTER_NAME, awsHelper.getAwsParamsDto());
        assertNull(clusterSummary);

        // Create the cluster.
        String clusterId = emrDao.createEmrCluster(EMR_CLUSTER_NAME, getTestEmrClusterDefinition(), awsHelper.getAwsParamsDto());

        // Check cluster summary after creation.
        clusterSummary = emrDao.getActiveEmrClusterByName(EMR_CLUSTER_NAME, awsHelper.getAwsParamsDto());
        assertNotNull(clusterSummary);
        assertEquals(clusterId, clusterSummary.getId());

        // Terminate the cluster.
        emrDao.terminateEmrCluster(EMR_CLUSTER_NAME, true, awsHelper.getAwsParamsDto());

        // Check the cluster status after termination.
        clusterSummary = emrDao.getActiveEmrClusterByName(EMR_CLUSTER_NAME, awsHelper.getAwsParamsDto());
        assertNull(clusterSummary);
    }

    /**
     * Gets an EMR cluster definition object.
     *
     * @return the EMR cluster definition object
     * @throws Exception if fails to load or unmarshall the cluster definition
     */
    private EmrClusterDefinition getTestEmrClusterDefinition() throws Exception
    {
        String configXml = IOUtils.toString(resourceLoader.getResource(AbstractDaoTest.EMR_CLUSTER_DEFINITION_XML_FILE_WITH_CLASSPATH).getInputStream());
        return xmlHelper.unmarshallXmlToObject(EmrClusterDefinition.class, configXml);
    }
}
