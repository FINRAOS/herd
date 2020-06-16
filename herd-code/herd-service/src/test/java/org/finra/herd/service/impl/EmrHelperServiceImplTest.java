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
package org.finra.herd.service.impl;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.elasticmapreduce.model.ClusterStatus;
import com.amazonaws.services.elasticmapreduce.model.ClusterSummary;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.EmrDao;
import org.finra.herd.dao.HerdDao;
import org.finra.herd.dao.helper.EmrHelper;
import org.finra.herd.dao.helper.EmrPricingHelper;
import org.finra.herd.dao.helper.XmlHelper;
import org.finra.herd.model.api.xml.EmrClusterCreateRequest;
import org.finra.herd.model.api.xml.EmrClusterDefinition;
import org.finra.herd.model.api.xml.EmrClusterDefinitionApplication;
import org.finra.herd.model.api.xml.EmrClusterDefinitionConfiguration;
import org.finra.herd.model.api.xml.EmrClusterDefinitionInstanceFleet;
import org.finra.herd.model.api.xml.EmrClusterDefinitionKerberosAttributes;
import org.finra.herd.model.api.xml.EmrClusterDefinitionKey;
import org.finra.herd.model.api.xml.HadoopJarStep;
import org.finra.herd.model.api.xml.InstanceDefinitions;
import org.finra.herd.model.api.xml.NodeTag;
import org.finra.herd.model.api.xml.Parameter;
import org.finra.herd.model.api.xml.ScriptDefinition;
import org.finra.herd.model.dto.AwsParamsDto;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.EmrClusterAlternateKeyDto;
import org.finra.herd.model.jpa.EmrClusterCreationLogEntity;
import org.finra.herd.model.jpa.EmrClusterDefinitionEntity;
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.service.AbstractServiceTest;
import org.finra.herd.service.helper.AwsServiceHelper;
import org.finra.herd.service.helper.EmrClusterDefinitionDaoHelper;
import org.finra.herd.service.helper.EmrClusterDefinitionHelper;
import org.finra.herd.service.helper.NamespaceDaoHelper;
import org.finra.herd.service.helper.NamespaceIamRoleAuthorizationHelper;

/**
 * This class tests functionality within the EMR helper service implementation.
 */
public class EmrHelperServiceImplTest extends AbstractServiceTest
{
    @Mock
    private AwsServiceHelper awsServiceHelper;

    @Mock
    private ConfigurationHelper configurationHelper;

    @Mock
    private EmrClusterDefinitionDaoHelper emrClusterDefinitionDaoHelper;

    @Mock
    private EmrClusterDefinitionHelper emrClusterDefinitionHelper;

    @Mock
    private EmrDao emrDao;

    @Mock
    private EmrHelper emrHelper;

    @InjectMocks
    private EmrHelperServiceImpl emrHelperServiceImpl;

    @Mock
    private EmrPricingHelper emrPricingHelper;

    @Mock
    private HerdDao herdDao;

    @Mock
    private NamespaceDaoHelper namespaceDaoHelper;

    @Mock
    private NamespaceIamRoleAuthorizationHelper namespaceIamRoleAuthorizationHelper;

    @Mock
    private XmlHelper xmlHelper;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testEmrCreateClusterAwsSpecificStepsImpl()
    {
        // Create an AWS params DTO
        AwsParamsDto awsParamsDto = new AwsParamsDto();

        // Create an EMR cluster definition object
        EmrClusterDefinition emrClusterDefinition = new EmrClusterDefinition();
        emrClusterDefinition.setAccountId(AWS_ACCOUNT_ID);
        emrClusterDefinition.setInstanceDefinitions(new InstanceDefinitions());

        // Create an EMR cluster create request
        EmrClusterCreateRequest emrClusterCreateRequest =
            new EmrClusterCreateRequest(NAMESPACE, EMR_CLUSTER_DEFINITION_NAME, EMR_CLUSTER_NAME, NO_DRY_RUN, emrClusterDefinition);
        emrClusterCreateRequest.setEmrClusterDefinitionOverride(emrClusterDefinition);

        // Create an EMR cluster alternate key DTO
        EmrClusterAlternateKeyDto emrClusterAlternateKeyDto = new EmrClusterAlternateKeyDto(NAMESPACE, EMR_CLUSTER_DEFINITION_NAME, EMR_CLUSTER_NAME);

        // Create a cluster summary object
        ClusterSummary clusterSummary = new ClusterSummary();
        clusterSummary.setId(EMR_CLUSTER_ID);
        clusterSummary.setStatus(new ClusterStatus().withState(EMR_CLUSTER_STATUS));

        // Mock the external calls.
        when(emrHelper.getAwsParamsDtoByAccountId(emrClusterDefinition.getAccountId())).thenReturn(awsParamsDto);
        when(emrHelper.isInstanceDefinitionsEmpty(emrClusterDefinition.getInstanceDefinitions())).thenReturn(false);
        when(emrHelper.buildEmrClusterName(emrClusterAlternateKeyDto.getNamespace(), emrClusterAlternateKeyDto.getEmrClusterDefinitionName(),
            emrClusterAlternateKeyDto.getEmrClusterName())).thenReturn(EMR_CLUSTER_NAME);
        when(emrDao.getActiveEmrClusterByNameAndAccountId(EMR_CLUSTER_NAME, emrClusterDefinition.getAccountId(), awsParamsDto)).thenReturn(clusterSummary);

        // Call the method under test.
        emrHelperServiceImpl.emrCreateClusterAwsSpecificSteps(emrClusterCreateRequest, emrClusterDefinition, emrClusterAlternateKeyDto);

        // Verify the external calls.
        verify(emrHelper).getAwsParamsDtoByAccountId(emrClusterDefinition.getAccountId());
        verify(emrHelper).isInstanceDefinitionsEmpty(emrClusterDefinition.getInstanceDefinitions());
        verify(emrPricingHelper).updateEmrClusterDefinitionWithBestPrice(emrClusterAlternateKeyDto, emrClusterDefinition, awsParamsDto);
        verify(emrHelper).buildEmrClusterName(emrClusterAlternateKeyDto.getNamespace(), emrClusterAlternateKeyDto.getEmrClusterDefinitionName(),
            emrClusterAlternateKeyDto.getEmrClusterName());
        verify(emrDao).getActiveEmrClusterByNameAndAccountId(EMR_CLUSTER_NAME, emrClusterDefinition.getAccountId(), awsParamsDto);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testEmrCreateClusterAwsSpecificStepsImplNullClusterSummary()
    {
        // Create an AWS params DTO
        AwsParamsDto awsParamsDto = new AwsParamsDto();

        // Create an EMR cluster definition object
        EmrClusterDefinition emrClusterDefinition = new EmrClusterDefinition();
        emrClusterDefinition.setAccountId(AWS_ACCOUNT_ID);
        emrClusterDefinition.setInstanceDefinitions(new InstanceDefinitions());

        // Create an EMR cluster create request
        EmrClusterCreateRequest emrClusterCreateRequest =
            new EmrClusterCreateRequest(NAMESPACE, EMR_CLUSTER_DEFINITION_NAME, EMR_CLUSTER_NAME, NO_DRY_RUN, emrClusterDefinition);
        emrClusterCreateRequest.setEmrClusterDefinitionOverride(emrClusterDefinition);

        // Create an EMR cluster alternate key DTO
        EmrClusterAlternateKeyDto emrClusterAlternateKeyDto = new EmrClusterAlternateKeyDto(NAMESPACE, EMR_CLUSTER_DEFINITION_NAME, EMR_CLUSTER_NAME);

        // Mock the external calls.
        when(emrHelper.getAwsParamsDtoByAccountId(emrClusterDefinition.getAccountId())).thenReturn(awsParamsDto);
        when(emrHelper.isInstanceDefinitionsEmpty(emrClusterDefinition.getInstanceDefinitions())).thenReturn(false);
        when(emrHelper.buildEmrClusterName(emrClusterAlternateKeyDto.getNamespace(), emrClusterAlternateKeyDto.getEmrClusterDefinitionName(),
            emrClusterAlternateKeyDto.getEmrClusterName())).thenReturn(EMR_CLUSTER_NAME);
        when(emrDao.getActiveEmrClusterByNameAndAccountId(EMR_CLUSTER_NAME, emrClusterDefinition.getAccountId(), awsParamsDto)).thenReturn(null);
        when(emrDao.createEmrCluster(EMR_CLUSTER_NAME, emrClusterDefinition, awsParamsDto)).thenReturn(EMR_CLUSTER_ID);
        when(emrDao.getEmrClusterStatusById(EMR_CLUSTER_ID, awsParamsDto)).thenReturn(EMR_CLUSTER_STATUS);

        // Call the method under test.
        emrHelperServiceImpl.emrCreateClusterAwsSpecificSteps(emrClusterCreateRequest, emrClusterDefinition, emrClusterAlternateKeyDto);

        // Verify the external calls.
        verify(emrHelper).getAwsParamsDtoByAccountId(emrClusterDefinition.getAccountId());
        verify(emrHelper).isInstanceDefinitionsEmpty(emrClusterDefinition.getInstanceDefinitions());
        verify(emrPricingHelper).updateEmrClusterDefinitionWithBestPrice(emrClusterAlternateKeyDto, emrClusterDefinition, awsParamsDto);
        verify(emrHelper).buildEmrClusterName(emrClusterAlternateKeyDto.getNamespace(), emrClusterAlternateKeyDto.getEmrClusterDefinitionName(),
            emrClusterAlternateKeyDto.getEmrClusterName());
        verify(emrDao).getActiveEmrClusterByNameAndAccountId(EMR_CLUSTER_NAME, emrClusterDefinition.getAccountId(), awsParamsDto);
        verify(emrDao).createEmrCluster(EMR_CLUSTER_NAME, emrClusterDefinition, awsParamsDto);
        verify(emrDao).getEmrClusterStatusById(EMR_CLUSTER_ID, awsParamsDto);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testEmrCreateClusterAwsSpecificStepsImplDryRun()
    {
        // Create an AWS params DTO
        AwsParamsDto awsParamsDto = new AwsParamsDto();

        // Create an EMR cluster definition object
        EmrClusterDefinition emrClusterDefinition = new EmrClusterDefinition();
        emrClusterDefinition.setAccountId(AWS_ACCOUNT_ID);
        emrClusterDefinition.setInstanceDefinitions(new InstanceDefinitions());

        // Create an EMR cluster create request
        EmrClusterCreateRequest emrClusterCreateRequest =
            new EmrClusterCreateRequest(NAMESPACE, EMR_CLUSTER_DEFINITION_NAME, EMR_CLUSTER_NAME, DRY_RUN, emrClusterDefinition);
        emrClusterCreateRequest.setEmrClusterDefinitionOverride(emrClusterDefinition);

        // Create an EMR cluster alternate key DTO
        EmrClusterAlternateKeyDto emrClusterAlternateKeyDto = new EmrClusterAlternateKeyDto(NAMESPACE, EMR_CLUSTER_DEFINITION_NAME, EMR_CLUSTER_NAME);

        // Create a cluster summary object
        ClusterSummary clusterSummary = new ClusterSummary();
        clusterSummary.setId(EMR_CLUSTER_ID);

        // Mock the external calls.
        when(emrHelper.getAwsParamsDtoByAccountId(emrClusterDefinition.getAccountId())).thenReturn(awsParamsDto);
        when(emrHelper.isInstanceDefinitionsEmpty(emrClusterDefinition.getInstanceDefinitions())).thenReturn(false);

        // Call the method under test.
        emrHelperServiceImpl.emrCreateClusterAwsSpecificSteps(emrClusterCreateRequest, emrClusterDefinition, emrClusterAlternateKeyDto);

        // Verify the external calls.
        verify(emrHelper).getAwsParamsDtoByAccountId(emrClusterDefinition.getAccountId());
        verify(emrHelper).isInstanceDefinitionsEmpty(emrClusterDefinition.getInstanceDefinitions());
        verify(emrPricingHelper).updateEmrClusterDefinitionWithBestPrice(emrClusterAlternateKeyDto, emrClusterDefinition, awsParamsDto);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testEmrCreateClusterAwsSpecificStepsImplWithAmazonServiceException()
    {
        // Create an AWS params DTO
        AwsParamsDto awsParamsDto = new AwsParamsDto();

        // Create an EMR cluster definition object
        EmrClusterDefinition emrClusterDefinition = new EmrClusterDefinition();
        emrClusterDefinition.setAccountId(AWS_ACCOUNT_ID);
        emrClusterDefinition.setInstanceDefinitions(new InstanceDefinitions());

        // Create an EMR cluster create request
        EmrClusterCreateRequest emrClusterCreateRequest =
            new EmrClusterCreateRequest(NAMESPACE, EMR_CLUSTER_DEFINITION_NAME, EMR_CLUSTER_NAME, NO_DRY_RUN, emrClusterDefinition);
        emrClusterCreateRequest.setEmrClusterDefinitionOverride(emrClusterDefinition);

        // Create an EMR cluster alternate key DTO
        EmrClusterAlternateKeyDto emrClusterAlternateKeyDto = new EmrClusterAlternateKeyDto(NAMESPACE, EMR_CLUSTER_DEFINITION_NAME, EMR_CLUSTER_NAME);

        AmazonServiceException amazonServiceException = new AmazonServiceException(ERROR_MESSAGE);

        // Mock the external calls.
        when(emrHelper.getAwsParamsDtoByAccountId(emrClusterDefinition.getAccountId())).thenReturn(awsParamsDto);
        when(emrHelper.isInstanceDefinitionsEmpty(emrClusterDefinition.getInstanceDefinitions())).thenReturn(false);
        when(emrHelper.buildEmrClusterName(emrClusterAlternateKeyDto.getNamespace(), emrClusterAlternateKeyDto.getEmrClusterDefinitionName(),
            emrClusterAlternateKeyDto.getEmrClusterName())).thenReturn(EMR_CLUSTER_NAME);
        when(emrDao.getActiveEmrClusterByNameAndAccountId(EMR_CLUSTER_NAME, emrClusterDefinition.getAccountId(), awsParamsDto)).thenReturn(null);
        when(emrDao.createEmrCluster(EMR_CLUSTER_NAME, emrClusterDefinition, awsParamsDto)).thenThrow(amazonServiceException);

        // Call the method under test.
        emrHelperServiceImpl.emrCreateClusterAwsSpecificSteps(emrClusterCreateRequest, emrClusterDefinition, emrClusterAlternateKeyDto);

        // Verify the external calls.
        verify(emrHelper).getAwsParamsDtoByAccountId(emrClusterDefinition.getAccountId());
        verify(emrHelper).isInstanceDefinitionsEmpty(emrClusterDefinition.getInstanceDefinitions());
        verify(emrPricingHelper).updateEmrClusterDefinitionWithBestPrice(emrClusterAlternateKeyDto, emrClusterDefinition, awsParamsDto);
        verify(emrHelper).buildEmrClusterName(emrClusterAlternateKeyDto.getNamespace(), emrClusterAlternateKeyDto.getEmrClusterDefinitionName(),
            emrClusterAlternateKeyDto.getEmrClusterName());
        verify(emrDao).getActiveEmrClusterByNameAndAccountId(EMR_CLUSTER_NAME, emrClusterDefinition.getAccountId(), awsParamsDto);
        verify(emrDao).createEmrCluster(EMR_CLUSTER_NAME, emrClusterDefinition, awsParamsDto);
        verify(awsServiceHelper)
            .handleAmazonException(amazonServiceException, "An Amazon exception occurred while creating EMR cluster with name \"" + EMR_CLUSTER_NAME + "\".");
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testEmrPreCreateClusterStepsImpl() throws Exception
    {
        // Create a namespace entity.
        NamespaceEntity namespaceEntity = new NamespaceEntity();
        namespaceEntity.setCode(NAMESPACE);

        // Create an EMR cluster definition object
        EmrClusterDefinition emrClusterDefinition = new EmrClusterDefinition();
        emrClusterDefinition.setServiceIamRole(SERVICE_IAM_ROLE);
        emrClusterDefinition.setEc2NodeIamProfileName(EC2_NODE_IAM_PROFILE_NAME);

        // Create an EMR cluster create request
        EmrClusterCreateRequest emrClusterCreateRequest =
            new EmrClusterCreateRequest(NAMESPACE, EMR_CLUSTER_DEFINITION_NAME, EMR_CLUSTER_NAME, NO_DRY_RUN, emrClusterDefinition);
        emrClusterCreateRequest.setEmrClusterDefinitionOverride(emrClusterDefinition);

        // Create an EMR cluster alternate key DTO
        EmrClusterAlternateKeyDto emrClusterAlternateKeyDto = new EmrClusterAlternateKeyDto(NAMESPACE, EMR_CLUSTER_DEFINITION_NAME, EMR_CLUSTER_NAME);

        // Create an EMR cluster definition key.
        EmrClusterDefinitionKey emrClusterDefinitionKey = new EmrClusterDefinitionKey(NAMESPACE, EMR_CLUSTER_DEFINITION_NAME);

        // Create an EMR cluster definition entity
        EmrClusterDefinitionEntity emrClusterDefinitionEntity = new EmrClusterDefinitionEntity();
        emrClusterDefinitionEntity.setNamespace(namespaceEntity);
        emrClusterDefinitionEntity.setConfiguration(EMR_CLUSTER_CONFIGURATION);

        // Mock the external calls.
        when(emrClusterDefinitionDaoHelper.getEmrClusterDefinitionEntity(emrClusterDefinitionKey)).thenReturn(emrClusterDefinitionEntity);
        when(configurationHelper.getProperty(ConfigurationValue.S3_STAGING_RESOURCE_LOCATION))
            .thenReturn((String) ConfigurationValue.S3_STAGING_RESOURCE_LOCATION.getDefaultValue());
        when(emrHelper.getS3StagingLocation()).thenReturn(S3_STAGING_LOCATION);
        when(xmlHelper.unmarshallXmlToObject(EmrClusterDefinition.class, EMR_CLUSTER_CONFIGURATION)).thenReturn(emrClusterDefinition);

        // Call the method under test.
        emrHelperServiceImpl.emrPreCreateClusterSteps(emrClusterAlternateKeyDto, emrClusterCreateRequest);

        // Verify the external calls.
        verify(emrClusterDefinitionDaoHelper).getEmrClusterDefinitionEntity(emrClusterDefinitionKey);
        verify(configurationHelper).getProperty(ConfigurationValue.S3_STAGING_RESOURCE_LOCATION);
        verify(emrHelper).getS3StagingLocation();
        verify(xmlHelper).unmarshallXmlToObject(EmrClusterDefinition.class, EMR_CLUSTER_CONFIGURATION);
        verify(emrClusterDefinitionHelper).validateEmrClusterDefinitionConfiguration(emrClusterDefinition);
        verify(namespaceIamRoleAuthorizationHelper).checkPermissions(emrClusterDefinitionEntity.getNamespace(), emrClusterDefinition.getServiceIamRole(),
            emrClusterDefinition.getEc2NodeIamProfileName());
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testLogEmrClusterCreationImpl() throws Exception
    {
        // Create a namespace entity.
        NamespaceEntity namespaceEntity = new NamespaceEntity();
        namespaceEntity.setCode(NAMESPACE);

        // Create an EMR cluster alternate key DTO
        EmrClusterAlternateKeyDto emrClusterAlternateKeyDto = new EmrClusterAlternateKeyDto(NAMESPACE, EMR_CLUSTER_DEFINITION_NAME, EMR_CLUSTER_NAME);

        // Create an EMR cluster definition object
        EmrClusterDefinition emrClusterDefinition = new EmrClusterDefinition();

        // Mock the external calls.
        when(namespaceDaoHelper.getNamespaceEntity(NAMESPACE)).thenReturn(namespaceEntity);
        when(xmlHelper.objectToXml(emrClusterDefinition)).thenReturn(EMR_CLUSTER_DEFINITION_XML_FILE_MINIMAL_CLASSPATH);

        // Call the method under test.
        emrHelperServiceImpl.logEmrClusterCreation(emrClusterAlternateKeyDto, emrClusterDefinition, EMR_CLUSTER_ID);

        // Verify the external calls.
        verify(namespaceDaoHelper).getNamespaceEntity(NAMESPACE);
        verify(xmlHelper).objectToXml(emrClusterDefinition);
        verify(herdDao).saveAndRefresh(any(EmrClusterCreationLogEntity.class));
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testOverrideEmrClusterDefinition()
    {
        EmrClusterDefinition emrClusterDefinition =
            new EmrClusterDefinition("sshKeyPairName", "subnetId", "logBucket", true, true, true, true, "accountId", "serviceIamRole", "ec2NodeIamProfileName",
                "amiVersion", "releaseLabel", "hadoopVersion", "hiveVersion", "pigVersion", true, Lists.newArrayList(new ScriptDefinition()),
                Lists.newArrayList(new ScriptDefinition()), "additionalInfo", new InstanceDefinitions(), 0,
                Lists.newArrayList(new EmrClusterDefinitionInstanceFleet()), Lists.newArrayList(new NodeTag()), "supportedProduct",
                Lists.newArrayList(new EmrClusterDefinitionApplication()), Lists.newArrayList(new EmrClusterDefinitionConfiguration()),
                Lists.newArrayList(new Parameter()), Lists.newArrayList(new Byte("0")), Lists.newArrayList(new HadoopJarStep()),
                Lists.newArrayList("additionalMasterSecurityGroups"), Lists.newArrayList("additionalSlaveSecurityGroups"), "securityConfiguration",
                "masterSecurityGroup", "slaveSecurityGroup", "serviceAccessSecurityGroup","scaleDownBehavior", new EmrClusterDefinitionKerberosAttributes());

        EmrClusterDefinition emrClusterDefinitionOverride =
            new EmrClusterDefinition("sshKeyPairNameOverride", "subnetIdOverride", "logBucketOverride", false, false, false, false, "accountIdOverride",
                "serviceIamRoleOverride", "ec2NodeIamProfileNameOverride", "amiVersionOverride", "releaseLabelOverride", "hadoopVersionOverride",
                "hiveVersionOverride", "pigVersionOverride", false, Lists.newArrayList(new ScriptDefinition(), new ScriptDefinition()),
                Lists.newArrayList(new ScriptDefinition(), new ScriptDefinition()), "additionalInfoOverride", new InstanceDefinitions(), 0,
                Lists.newArrayList(new EmrClusterDefinitionInstanceFleet(), new EmrClusterDefinitionInstanceFleet()),
                Lists.newArrayList(new NodeTag(), new NodeTag()), "supportedProductOverride",
                Lists.newArrayList(new EmrClusterDefinitionApplication(), new EmrClusterDefinitionApplication()),
                Lists.newArrayList(new EmrClusterDefinitionConfiguration(), new EmrClusterDefinitionConfiguration()),
                Lists.newArrayList(new Parameter(), new Parameter()), Lists.newArrayList(new Byte("0"), new Byte("0")),
                Lists.newArrayList(new HadoopJarStep(), new HadoopJarStep()),
                Lists.newArrayList("additionalMasterSecurityGroupsOverride", "additionalMasterSecurityGroupsOverride"),
                Lists.newArrayList("additionalSlaveSecurityGroupsOverride", "additionalSlaveSecurityGroupsOverride"), "securityConfigurationOverride",
                "masterSecurityGroupOverride", "slaveSecurityGroupOverride", "serviceSecurityGroupOverride", "scaleDownBehaviorOverride", new EmrClusterDefinitionKerberosAttributes());

        // Call the method under test.
        emrHelperServiceImpl.overrideEmrClusterDefinition(emrClusterDefinition, emrClusterDefinitionOverride);

        // Validate the override
        assertThat("Did not override.", emrClusterDefinition, is(emrClusterDefinitionOverride));
    }

    @Test
    public void testOverrideEmrClusterDefinitionNoOverride()
    {
        EmrClusterDefinition emrClusterDefinition =
            new EmrClusterDefinition("sshKeyPairName", "subnetId", "logBucket", true, true, true, true, "accountId", "serviceIamRole", "ec2NodeIamProfileName",
                "amiVersion", "releaseLabel", "hadoopVersion", "hiveVersion", "pigVersion", true, Lists.newArrayList(new ScriptDefinition()),
                Lists.newArrayList(new ScriptDefinition()), "additionalInfo", new InstanceDefinitions(), 0,
                Lists.newArrayList(new EmrClusterDefinitionInstanceFleet()), Lists.newArrayList(new NodeTag()), "supportedProduct",
                Lists.newArrayList(new EmrClusterDefinitionApplication()), Lists.newArrayList(new EmrClusterDefinitionConfiguration()),
                Lists.newArrayList(new Parameter()), Lists.newArrayList(new Byte("0")), Lists.newArrayList(new HadoopJarStep()),
                Lists.newArrayList("additionalMasterSecurityGroups"), Lists.newArrayList("additionalSlaveSecurityGroups"), "securityConfiguration",
                "masterSecurityGroup", "slaveSecurityGroup", "serviceAccessSecurityGroup","scaleDownBehavior", new EmrClusterDefinitionKerberosAttributes());

        EmrClusterDefinition emrClusterDefinitionOverride = new EmrClusterDefinition();

        // Call the method under test.
        emrHelperServiceImpl.overrideEmrClusterDefinition(emrClusterDefinition, emrClusterDefinitionOverride);

        // Validate the override
        assertThat("Should not override.", emrClusterDefinition, is(emrClusterDefinition));
    }

    /**
     * Checks if any of the mocks has any interaction.
     */
    private void verifyNoMoreInteractionsHelper()
    {
        verifyNoMoreInteractions(awsServiceHelper, configurationHelper, emrClusterDefinitionDaoHelper, emrClusterDefinitionHelper, emrDao, emrHelper,
            emrPricingHelper, herdDao, namespaceDaoHelper, namespaceIamRoleAuthorizationHelper, xmlHelper);
    }
}
