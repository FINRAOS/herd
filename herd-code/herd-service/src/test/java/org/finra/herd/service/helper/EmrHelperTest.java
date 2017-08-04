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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.elasticmapreduce.model.Cluster;
import com.amazonaws.services.elasticmapreduce.model.ClusterSummary;
import org.apache.commons.io.IOUtils;
import org.junit.Test;

import org.finra.herd.dao.impl.MockEmrOperationsImpl;
import org.finra.herd.model.api.xml.EmrCluster;
import org.finra.herd.model.api.xml.EmrClusterCreateRequest;
import org.finra.herd.model.api.xml.EmrClusterDefinition;
import org.finra.herd.model.api.xml.EmrHadoopJarStep;
import org.finra.herd.model.api.xml.EmrHiveStep;
import org.finra.herd.model.api.xml.EmrPigStep;
import org.finra.herd.model.api.xml.EmrShellStep;
import org.finra.herd.model.api.xml.NodeTag;
import org.finra.herd.model.dto.AwsParamsDto;
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.service.AbstractServiceTest;

/**
 * This class tests functionality within the EmrHelper.
 */
public class EmrHelperTest extends AbstractServiceTest
{
    /**
     * This method tests the blank EC2 tags
     */
    @Test
    public void testCreateEmrClusterBlankTags() throws Exception
    {
        // Create the namespace entity.
        namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        String configXml = IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_WITH_CLASSPATH).getInputStream());

        EmrClusterDefinition emrClusterDefinition = xmlHelper.unmarshallXmlToObject(EmrClusterDefinition.class, configXml);

        // Add new node tags. First tag does not contain any tag value; but contains node tag name
        NodeTag nodeTag1 = new NodeTag();
        nodeTag1.setTagName("NO_TAG_VALUE");

        // Second tag does not contain node tag name; but contains node tag value
        NodeTag nodeTag2 = new NodeTag();
        nodeTag2.setTagValue("NO_TAG_NAME");

        // Create the list and add the tags
        ArrayList<NodeTag> nodeTagList = new ArrayList<>();
        nodeTagList.add(nodeTag1);
        nodeTagList.add(nodeTag2);

        // Set the tags
        emrClusterDefinition.setNodeTags(nodeTagList);
        emrDao.createEmrCluster(EMR_CLUSTER_DEFINITION_NAME, emrClusterDefinition, emrHelper.getAwsParamsDto());
    }

    /**
     * This method tests the negative test cases scenario by testing all the step types
     */
    @Test
    public void testEmrAddStepsAllTypesNegativeTestCase() throws Exception
    {
        // Create a namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        // Create an EMR cluster definition entity.
        emrClusterDefinitionDaoTestHelper.createEmrClusterDefinitionEntity(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME,
            IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_WITH_CLASSPATH).getInputStream()));

        // Create EC2 on-demand pricing entities.
        ec2OnDemandPricingDaoTestHelper.createEc2OnDemandPricingEntities();

        EmrClusterCreateRequest request = getNewEmrClusterCreateRequest();
        EmrCluster emrCluster = emrService.createCluster(request);

        EmrStepHelper stepHelper;

        // Shell script arguments
        List<String> shellScriptArgs = new ArrayList<>();
        shellScriptArgs.add("Hello");
        shellScriptArgs.add("herd");
        shellScriptArgs.add("How Are You");

        List<Object> steps = new ArrayList<>();

        // Shell step parameters
        EmrShellStep shellStep = new EmrShellStep(NAMESPACE, EMR_CLUSTER_DEFINITION_NAME, request.getEmrClusterName(), null, null, null, null, null, null);
        shellStep.setScriptLocation("s3://test-bucket-managed/app-a/test/test_script.sh");
        shellStep.setStepName("Test Shell Script");
        shellStep.setScriptArguments(shellScriptArgs);
        shellStep.setContinueOnError(true);

        steps.add(shellStep);

        // Hive step parameters
        EmrHiveStep hiveStep = new EmrHiveStep(NAMESPACE, EMR_CLUSTER_DEFINITION_NAME, request.getEmrClusterName(), null, null, null, null, null, null);
        hiveStep.setStepName("Test Hive");
        hiveStep.setScriptLocation("s3://test-bucket-managed/app-a/test/test_hive.hql");
        hiveStep.setContinueOnError(true);

        steps.add(hiveStep);

        // Pig step parameter
        EmrPigStep pigStep = new EmrPigStep(NAMESPACE, EMR_CLUSTER_DEFINITION_NAME, request.getEmrClusterName(), null, null, null, null, null, null);
        pigStep.setStepName("Test Pig");
        pigStep.setContinueOnError(true);
        pigStep.setScriptLocation("s3://test-bucket-managed/app-a/test/test_pig.pig");

        steps.add(pigStep);

        // Oozie step that includes a shell script to install oozie
        shellStep = new EmrShellStep(NAMESPACE, EMR_CLUSTER_DEFINITION_NAME, request.getEmrClusterName(), null, null, null, null, null, null);
        shellStep.setScriptLocation("s3://test-bucket-managed/app-a/bootstrap/install_oozie.sh");
        shellStep.setStepName("Install Oozie");
        List<String> shellScriptArgsOozie = new ArrayList<>();
        shellScriptArgsOozie.add("s3://test-bucket-managed/app-a/bootstrap/oozie-4.0.1-distro.tar");
        shellStep.setScriptArguments(shellScriptArgsOozie);

        steps.add(shellStep);

        // Hadoop jar step configuration
        EmrHadoopJarStep hadoopJarStep =
            new EmrHadoopJarStep(NAMESPACE, EMR_CLUSTER_DEFINITION_NAME, request.getEmrClusterName(), null, null, null, null, null, null, null);
        hadoopJarStep.setContinueOnError(true);
        hadoopJarStep.setStepName("Hadoop Jar");
        hadoopJarStep.setJarLocation("s3://test-bucket-managed/app-a/test/hadoop-mapreduce-examples-2.4.0.jar");
        hadoopJarStep.setMainClass("wordcount");

        steps.add(hadoopJarStep);

        for (Object emrStep : steps)
        {
            stepHelper = emrStepHelperFactory.getStepHelper(emrStep.getClass().getName());

            emrDao.addEmrStep(emrCluster.getId(), stepHelper.getEmrStepConfig(emrStep), emrHelper.getAwsParamsDto());
        }
    }

    /**
     * This method tests the blank proxy details
     */
    @Test
    public void testEmrAwsDtoBlankProxy() throws Exception
    {
        // Set the proxy as blank too to get the EMR client without proxy
        AwsParamsDto awsParamsDto = emrHelper.getAwsParamsDto();
        awsParamsDto.setHttpProxyHost("");
        emrDao.getEmrClient(awsParamsDto);
    }

    @Test
    public void testGetActiveEmrClusterByName() throws Exception
    {
        // Get the EMR cluster definition object
        String configXml = IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_WITH_CLASSPATH).getInputStream());
        EmrClusterDefinition emrClusterDefinition = xmlHelper.unmarshallXmlToObject(EmrClusterDefinition.class, configXml);

        // check cluster summary before creation
        ClusterSummary clusterSummary = emrDao.getActiveEmrClusterByName(MockEmrOperationsImpl.MOCK_CLUSTER_NAME, emrHelper.getAwsParamsDto());
        assertNull(clusterSummary);

        // Create the cluster
        String clusterId = emrDao.createEmrCluster(MockEmrOperationsImpl.MOCK_CLUSTER_NAME, emrClusterDefinition, emrHelper.getAwsParamsDto());

        // check cluster summary after creation
        clusterSummary = emrDao.getActiveEmrClusterByName(MockEmrOperationsImpl.MOCK_CLUSTER_NAME, emrHelper.getAwsParamsDto());
        assertNotNull(clusterSummary);
        assertEquals(clusterId, clusterSummary.getId());
    }

    @Test
    public void testGetEmrClusterByIdNull() throws Exception
    {
        Cluster cluster = emrDao.getEmrClusterById(null, null);

        assertNull(cluster);
    }

    /**
     * This method tests the blank cluster id for finding cluster status
     */
    @Test
    public void testGetEmrClusterStatusByIdWithBlank() throws Exception
    {
        // Send blank for cluster id, and this method returns null for describeClusterResult
        emrDao.getEmrClusterStatusById(EMR_CLUSTER_DEFINITION_NAME, emrHelper.getAwsParamsDto());
    }

    /**
     * This method fills-up the parameters required for the EMR cluster create request. This is called from all the other test methods.
     */
    private EmrClusterCreateRequest getNewEmrClusterCreateRequest() throws Exception
    {
        // Create the definition.
        EmrClusterCreateRequest request = new EmrClusterCreateRequest();

        // Fill in the parameters.
        request.setNamespace(NAMESPACE);
        request.setEmrClusterDefinitionName(EMR_CLUSTER_DEFINITION_NAME);
        request.setEmrClusterName("UT_EMR_CLUSTER" + String.format("-%.3f", Math.random()));

        return request;
    }
}
