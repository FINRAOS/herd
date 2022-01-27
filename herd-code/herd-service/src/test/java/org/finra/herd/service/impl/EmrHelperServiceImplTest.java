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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.ec2.model.Subnet;
import com.amazonaws.services.elasticmapreduce.model.ClusterStatus;
import com.amazonaws.services.elasticmapreduce.model.ClusterSummary;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.EmrDao;
import org.finra.herd.dao.HerdDao;
import org.finra.herd.dao.helper.EmrHelper;
import org.finra.herd.dao.helper.EmrPricingHelper;
import org.finra.herd.dao.helper.JsonHelper;
import org.finra.herd.dao.helper.XmlHelper;
import org.finra.herd.model.api.xml.EmrClusterCreateRequest;
import org.finra.herd.model.api.xml.EmrClusterDefinition;
import org.finra.herd.model.api.xml.EmrClusterDefinitionApplication;
import org.finra.herd.model.api.xml.EmrClusterDefinitionAutoTerminationPolicy;
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
import org.finra.herd.model.dto.EmrParamsDto;
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

    @Mock
    private JsonHelper jsonHelper;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testEmrCreateClusterAwsSpecificStepsImpl()
    {
        // Create an EMR cluster definition object.
        EmrClusterDefinition emrClusterDefinition = new EmrClusterDefinition();
        emrClusterDefinition.setAccountId(AWS_ACCOUNT_ID);
        emrClusterDefinition.setInstanceDefinitions(new InstanceDefinitions());

        // Create an EMR cluster create request.
        EmrClusterCreateRequest emrClusterCreateRequest =
            new EmrClusterCreateRequest(NAMESPACE, EMR_CLUSTER_DEFINITION_NAME, EMR_CLUSTER_NAME, NO_DRY_RUN, emrClusterDefinition);
        emrClusterCreateRequest.setEmrClusterDefinitionOverride(emrClusterDefinition);

        // Create an EMR cluster alternate key DTO.
        EmrClusterAlternateKeyDto emrClusterAlternateKeyDto = new EmrClusterAlternateKeyDto(NAMESPACE, EMR_CLUSTER_DEFINITION_NAME, EMR_CLUSTER_NAME);

        // Create an EMR parameters DTO.
        EmrParamsDto emrParamsDto =
            new EmrParamsDto(NO_AWS_ACCESS_KEY, NO_AWS_SECRET_KEY, NO_SESSION_TOKEN, HTTP_PROXY_HOST, HTTP_PROXY_PORT, AWS_REGION_NAME_US_EAST_1,
                NO_S3_TRUSTING_ACCOUNT_STAGING_BUCKET_NAME);

        // Create a cluster summary object.
        ClusterSummary clusterSummary = new ClusterSummary();
        clusterSummary.setId(EMR_CLUSTER_ID);
        clusterSummary.setStatus(new ClusterStatus().withState(EMR_CLUSTER_STATUS));

        // Mock the external calls.
        when(emrHelper.isInstanceDefinitionsEmpty(emrClusterDefinition.getInstanceDefinitions())).thenReturn(false);
        when(emrHelper.buildEmrClusterName(emrClusterAlternateKeyDto.getNamespace(), emrClusterAlternateKeyDto.getEmrClusterDefinitionName(),
            emrClusterAlternateKeyDto.getEmrClusterName())).thenReturn(EMR_CLUSTER_NAME);
        when(emrDao.getActiveEmrClusterByNameAndAccountId(EMR_CLUSTER_NAME, emrClusterDefinition.getAccountId(), emrParamsDto)).thenReturn(clusterSummary);

        // Call the method under test.
        emrHelperServiceImpl.emrCreateClusterAwsSpecificSteps(emrClusterCreateRequest, emrClusterDefinition, emrClusterAlternateKeyDto, emrParamsDto);

        // Verify the external calls.
        verify(emrHelper).isInstanceDefinitionsEmpty(emrClusterDefinition.getInstanceDefinitions());
        verify(emrPricingHelper).updateEmrClusterDefinitionWithBestPrice(emrClusterAlternateKeyDto, emrClusterDefinition, emrParamsDto);
        verify(emrHelper).buildEmrClusterName(emrClusterAlternateKeyDto.getNamespace(), emrClusterAlternateKeyDto.getEmrClusterDefinitionName(),
            emrClusterAlternateKeyDto.getEmrClusterName());
        verify(emrDao).getActiveEmrClusterByNameAndAccountId(EMR_CLUSTER_NAME, emrClusterDefinition.getAccountId(), emrParamsDto);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testEmrCreateClusterAwsSpecificStepsImplNullClusterSummary()
    {
        // Create an EMR cluster definition object.
        EmrClusterDefinition emrClusterDefinition = new EmrClusterDefinition();
        emrClusterDefinition.setAccountId(AWS_ACCOUNT_ID);
        emrClusterDefinition.setInstanceDefinitions(new InstanceDefinitions());

        // Create an EMR cluster create request.
        EmrClusterCreateRequest emrClusterCreateRequest =
            new EmrClusterCreateRequest(NAMESPACE, EMR_CLUSTER_DEFINITION_NAME, EMR_CLUSTER_NAME, NO_DRY_RUN, emrClusterDefinition);
        emrClusterCreateRequest.setEmrClusterDefinitionOverride(emrClusterDefinition);

        // Create an EMR cluster alternate key DTO.
        EmrClusterAlternateKeyDto emrClusterAlternateKeyDto = new EmrClusterAlternateKeyDto(NAMESPACE, EMR_CLUSTER_DEFINITION_NAME, EMR_CLUSTER_NAME);

        // Create an EMR parameters DTO.
        EmrParamsDto emrParamsDto =
            new EmrParamsDto(NO_AWS_ACCESS_KEY, NO_AWS_SECRET_KEY, NO_SESSION_TOKEN, HTTP_PROXY_HOST, HTTP_PROXY_PORT, AWS_REGION_NAME_US_EAST_1,
                NO_S3_TRUSTING_ACCOUNT_STAGING_BUCKET_NAME);

        // Mock the external calls.
        when(emrHelper.isInstanceDefinitionsEmpty(emrClusterDefinition.getInstanceDefinitions())).thenReturn(false);
        when(emrHelper.buildEmrClusterName(emrClusterAlternateKeyDto.getNamespace(), emrClusterAlternateKeyDto.getEmrClusterDefinitionName(),
            emrClusterAlternateKeyDto.getEmrClusterName())).thenReturn(EMR_CLUSTER_NAME);
        when(emrDao.getActiveEmrClusterByNameAndAccountId(EMR_CLUSTER_NAME, emrClusterDefinition.getAccountId(), emrParamsDto)).thenReturn(null);
        when(emrDao.createEmrCluster(EMR_CLUSTER_NAME, emrClusterDefinition, emrParamsDto)).thenReturn(EMR_CLUSTER_ID);
        when(emrDao.getEmrClusterStatusById(EMR_CLUSTER_ID, emrParamsDto)).thenReturn(EMR_CLUSTER_STATUS);

        // Call the method under test.
        emrHelperServiceImpl.emrCreateClusterAwsSpecificSteps(emrClusterCreateRequest, emrClusterDefinition, emrClusterAlternateKeyDto, emrParamsDto);

        // Verify the external calls.
        verify(emrHelper).isInstanceDefinitionsEmpty(emrClusterDefinition.getInstanceDefinitions());
        verify(emrPricingHelper).updateEmrClusterDefinitionWithBestPrice(emrClusterAlternateKeyDto, emrClusterDefinition, emrParamsDto);
        verify(emrHelper).buildEmrClusterName(emrClusterAlternateKeyDto.getNamespace(), emrClusterAlternateKeyDto.getEmrClusterDefinitionName(),
            emrClusterAlternateKeyDto.getEmrClusterName());
        verify(emrDao).getActiveEmrClusterByNameAndAccountId(EMR_CLUSTER_NAME, emrClusterDefinition.getAccountId(), emrParamsDto);
        verify(emrDao).createEmrCluster(EMR_CLUSTER_NAME, emrClusterDefinition, emrParamsDto);
        verify(emrDao).getEmrClusterStatusById(EMR_CLUSTER_ID, emrParamsDto);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testEmrCreateClusterAwsSpecificStepsImplDryRun()
    {
        // Create an EMR cluster definition object.
        EmrClusterDefinition emrClusterDefinition = new EmrClusterDefinition();
        emrClusterDefinition.setAccountId(AWS_ACCOUNT_ID);
        emrClusterDefinition.setInstanceDefinitions(new InstanceDefinitions());

        // Create an EMR cluster create request.
        EmrClusterCreateRequest emrClusterCreateRequest =
            new EmrClusterCreateRequest(NAMESPACE, EMR_CLUSTER_DEFINITION_NAME, EMR_CLUSTER_NAME, DRY_RUN, emrClusterDefinition);
        emrClusterCreateRequest.setEmrClusterDefinitionOverride(emrClusterDefinition);

        // Create an EMR cluster alternate key DTO.
        EmrClusterAlternateKeyDto emrClusterAlternateKeyDto = new EmrClusterAlternateKeyDto(NAMESPACE, EMR_CLUSTER_DEFINITION_NAME, EMR_CLUSTER_NAME);

        // Create an EMR parameters DTO.
        EmrParamsDto emrParamsDto =
            new EmrParamsDto(NO_AWS_ACCESS_KEY, NO_AWS_SECRET_KEY, NO_SESSION_TOKEN, HTTP_PROXY_HOST, HTTP_PROXY_PORT, AWS_REGION_NAME_US_EAST_1,
                NO_S3_TRUSTING_ACCOUNT_STAGING_BUCKET_NAME);

        // Create a cluster summary object
        ClusterSummary clusterSummary = new ClusterSummary();
        clusterSummary.setId(EMR_CLUSTER_ID);

        // Mock the external calls.
        when(emrHelper.isInstanceDefinitionsEmpty(emrClusterDefinition.getInstanceDefinitions())).thenReturn(false);

        // Call the method under test.
        emrHelperServiceImpl.emrCreateClusterAwsSpecificSteps(emrClusterCreateRequest, emrClusterDefinition, emrClusterAlternateKeyDto, emrParamsDto);

        // Verify the external calls.
        verify(emrHelper).isInstanceDefinitionsEmpty(emrClusterDefinition.getInstanceDefinitions());
        verify(emrPricingHelper).updateEmrClusterDefinitionWithBestPrice(emrClusterAlternateKeyDto, emrClusterDefinition, emrParamsDto);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testEmrCreateClusterAwsSpecificStepsImplWithAmazonServiceException()
    {
        // Create an EMR cluster definition object.
        EmrClusterDefinition emrClusterDefinition = new EmrClusterDefinition();
        emrClusterDefinition.setAccountId(AWS_ACCOUNT_ID);
        emrClusterDefinition.setInstanceDefinitions(new InstanceDefinitions());

        // Create an EMR cluster create request.
        EmrClusterCreateRequest emrClusterCreateRequest =
            new EmrClusterCreateRequest(NAMESPACE, EMR_CLUSTER_DEFINITION_NAME, EMR_CLUSTER_NAME, NO_DRY_RUN, emrClusterDefinition);
        emrClusterCreateRequest.setEmrClusterDefinitionOverride(emrClusterDefinition);

        // Create an EMR cluster alternate key DTO.
        EmrClusterAlternateKeyDto emrClusterAlternateKeyDto = new EmrClusterAlternateKeyDto(NAMESPACE, EMR_CLUSTER_DEFINITION_NAME, EMR_CLUSTER_NAME);

        // Create an EMR parameters DTO.
        EmrParamsDto emrParamsDto =
            new EmrParamsDto(NO_AWS_ACCESS_KEY, NO_AWS_SECRET_KEY, NO_SESSION_TOKEN, HTTP_PROXY_HOST, HTTP_PROXY_PORT, AWS_REGION_NAME_US_EAST_1,
                NO_S3_TRUSTING_ACCOUNT_STAGING_BUCKET_NAME);

        // Create an Amazon service exception.
        AmazonServiceException amazonServiceException = new AmazonServiceException(ERROR_MESSAGE);

        // Mock the external calls.
        when(emrHelper.isInstanceDefinitionsEmpty(emrClusterDefinition.getInstanceDefinitions())).thenReturn(false);
        when(emrHelper.buildEmrClusterName(emrClusterAlternateKeyDto.getNamespace(), emrClusterAlternateKeyDto.getEmrClusterDefinitionName(),
            emrClusterAlternateKeyDto.getEmrClusterName())).thenReturn(EMR_CLUSTER_NAME);
        when(emrDao.getActiveEmrClusterByNameAndAccountId(EMR_CLUSTER_NAME, emrClusterDefinition.getAccountId(), emrParamsDto)).thenReturn(null);
        when(emrDao.createEmrCluster(EMR_CLUSTER_NAME, emrClusterDefinition, emrParamsDto)).thenThrow(amazonServiceException);

        // Call the method under test.
        emrHelperServiceImpl.emrCreateClusterAwsSpecificSteps(emrClusterCreateRequest, emrClusterDefinition, emrClusterAlternateKeyDto, emrParamsDto);

        // Verify the external calls.
        verify(emrHelper).isInstanceDefinitionsEmpty(emrClusterDefinition.getInstanceDefinitions());
        verify(emrPricingHelper).updateEmrClusterDefinitionWithBestPrice(emrClusterAlternateKeyDto, emrClusterDefinition, emrParamsDto);
        verify(emrHelper).buildEmrClusterName(emrClusterAlternateKeyDto.getNamespace(), emrClusterAlternateKeyDto.getEmrClusterDefinitionName(),
            emrClusterAlternateKeyDto.getEmrClusterName());
        verify(emrDao).getActiveEmrClusterByNameAndAccountId(EMR_CLUSTER_NAME, emrClusterDefinition.getAccountId(), emrParamsDto);
        verify(emrDao).createEmrCluster(EMR_CLUSTER_NAME, emrClusterDefinition, emrParamsDto);
        verify(awsServiceHelper)
            .handleAmazonException(amazonServiceException, "An Amazon exception occurred while creating EMR cluster with name \"" + EMR_CLUSTER_NAME + "\".");
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testEmrPreCreateClusterStepsImpl() throws Exception
    {
        // Create an override for EMR cluster definition.
        EmrClusterDefinition emrClusterDefinitionOverride = new EmrClusterDefinition();
        emrClusterDefinitionOverride.setAccountId(AWS_ACCOUNT_ID_2);
        emrClusterDefinitionOverride.setAdditionalInfo(STRING_VALUE_2);

        // Create an EMR cluster create request.
        EmrClusterCreateRequest emrClusterCreateRequest =
            new EmrClusterCreateRequest(NAMESPACE, EMR_CLUSTER_DEFINITION_NAME, EMR_CLUSTER_NAME, NO_DRY_RUN, emrClusterDefinitionOverride);

        // Create an EMR cluster alternate key DTO.
        EmrClusterAlternateKeyDto emrClusterAlternateKeyDto = new EmrClusterAlternateKeyDto(NAMESPACE, EMR_CLUSTER_DEFINITION_NAME, EMR_CLUSTER_NAME);

        // Create an EMR cluster definition key.
        EmrClusterDefinitionKey emrClusterDefinitionKey = new EmrClusterDefinitionKey(NAMESPACE, EMR_CLUSTER_DEFINITION_NAME);

        // Create a namespace entity.
        NamespaceEntity namespaceEntity = new NamespaceEntity();
        namespaceEntity.setCode(NAMESPACE);

        // Create an EMR cluster definition entity
        EmrClusterDefinitionEntity emrClusterDefinitionEntity = new EmrClusterDefinitionEntity();
        emrClusterDefinitionEntity.setNamespace(namespaceEntity);
        emrClusterDefinitionEntity.setConfiguration(EMR_CLUSTER_CONFIGURATION_XML_1);

        // Create an original EMR cluster definition.
        EmrClusterDefinition originalEmrClusterDefinition = new EmrClusterDefinition();
        originalEmrClusterDefinition.setAdditionalInfo(STRING_VALUE);
        originalEmrClusterDefinition.setAccountId(AWS_ACCOUNT_ID);

        // Create a final (updated) EMR cluster definition.
        EmrClusterDefinition finalEmrClusterDefinition = new EmrClusterDefinition();
        finalEmrClusterDefinition.setServiceIamRole(SERVICE_IAM_ROLE);
        finalEmrClusterDefinition.setEc2NodeIamProfileName(EC2_NODE_IAM_PROFILE_NAME);

        // Create an EMR parameters DTO.
        EmrParamsDto emrParamsDto =
            new EmrParamsDto(NO_AWS_ACCESS_KEY, NO_AWS_SECRET_KEY, NO_SESSION_TOKEN, HTTP_PROXY_HOST, HTTP_PROXY_PORT, AWS_REGION_NAME_US_EAST_1,
                S3_TRUSTING_ACCOUNT_STAGING_BUCKET_NAME);

        // Mock the external calls.
        when(emrClusterDefinitionDaoHelper.getEmrClusterDefinitionEntity(emrClusterDefinitionKey)).thenReturn(emrClusterDefinitionEntity);
        when(xmlHelper.unmarshallXmlToObject(EmrClusterDefinition.class, EMR_CLUSTER_CONFIGURATION_XML_1)).thenReturn(originalEmrClusterDefinition);
        when(emrHelper.getEmrParamsDtoByAccountId(AWS_ACCOUNT_ID_2)).thenReturn(emrParamsDto);
        when(xmlHelper.objectToXml(originalEmrClusterDefinition)).thenReturn(EMR_CLUSTER_CONFIGURATION_XML_2);
        when(configurationHelper.getProperty(ConfigurationValue.S3_STAGING_RESOURCE_LOCATION))
            .thenReturn((String) ConfigurationValue.S3_STAGING_RESOURCE_LOCATION.getDefaultValue());
        when(emrHelper.getS3StagingLocation(S3_TRUSTING_ACCOUNT_STAGING_BUCKET_NAME)).thenReturn(S3_STAGING_LOCATION);
        when(xmlHelper.unmarshallXmlToObject(EmrClusterDefinition.class, EMR_CLUSTER_CONFIGURATION_XML_2)).thenReturn(finalEmrClusterDefinition);

        // Call the method under test.
        emrHelperServiceImpl.emrPreCreateClusterSteps(emrClusterAlternateKeyDto, emrClusterCreateRequest);

        // Verify the external calls.
        verify(emrClusterDefinitionDaoHelper).getEmrClusterDefinitionEntity(emrClusterDefinitionKey);
        verify(xmlHelper).unmarshallXmlToObject(EmrClusterDefinition.class, EMR_CLUSTER_CONFIGURATION_XML_1);
        verify(emrHelper).getEmrParamsDtoByAccountId(AWS_ACCOUNT_ID_2);
        verify(xmlHelper).objectToXml(originalEmrClusterDefinition);
        verify(configurationHelper).getProperty(ConfigurationValue.S3_STAGING_RESOURCE_LOCATION);
        verify(emrHelper).getS3StagingLocation(S3_TRUSTING_ACCOUNT_STAGING_BUCKET_NAME);
        verify(xmlHelper).unmarshallXmlToObject(EmrClusterDefinition.class, EMR_CLUSTER_CONFIGURATION_XML_2);
        verify(emrClusterDefinitionHelper).validateEmrClusterDefinitionConfiguration(finalEmrClusterDefinition);
        verify(namespaceIamRoleAuthorizationHelper).checkPermissions(namespaceEntity, SERVICE_IAM_ROLE, EC2_NODE_IAM_PROFILE_NAME);
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
    public void testUpdateEmrClusterDefinitionWithNullFilterValue()
    {
        // Create an AWS params DTO
        AwsParamsDto awsParamsDto = new AwsParamsDto();

        // Create an EMR cluster alternate key DTO
        EmrClusterAlternateKeyDto emrClusterAlternateKeyDto = new EmrClusterAlternateKeyDto(NAMESPACE, EMR_CLUSTER_DEFINITION_NAME, EMR_CLUSTER_NAME);

        // Create an EMR cluster definition object
        EmrClusterDefinition emrClusterDefinition = new EmrClusterDefinition();
        emrClusterDefinition.setInstanceFleetMinimumIpAvailableFilter(null);
        emrClusterDefinition.setSubnetId("Test_Subnet_1,Test_Subnet_2");

        // Call the method under test.
        emrHelperServiceImpl.updateEmrClusterDefinitionWithValidInstanceFleetSubnets(emrClusterAlternateKeyDto, emrClusterDefinition, awsParamsDto);

        // Verify correct value
        assertEquals("Test_Subnet_1,Test_Subnet_2", emrClusterDefinition.getSubnetId());

        // Verify the external calls.
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testUpdateEmrClusterDefinitionWithZeroFilterValue()
    {
        // Create an AWS params DTO
        AwsParamsDto awsParamsDto = new AwsParamsDto();

        // Create an EMR cluster alternate key DTO
        EmrClusterAlternateKeyDto emrClusterAlternateKeyDto = new EmrClusterAlternateKeyDto(NAMESPACE, EMR_CLUSTER_DEFINITION_NAME, EMR_CLUSTER_NAME);

        // Create an EMR cluster definition object
        EmrClusterDefinition emrClusterDefinition = new EmrClusterDefinition();
        emrClusterDefinition.setInstanceFleetMinimumIpAvailableFilter(0);
        emrClusterDefinition.setSubnetId("Test_Subnet_1,Test_Subnet_2");

        // Call the method under test.
        emrHelperServiceImpl.updateEmrClusterDefinitionWithValidInstanceFleetSubnets(emrClusterAlternateKeyDto, emrClusterDefinition, awsParamsDto);

        // Verify correct value
        assertEquals("Test_Subnet_1,Test_Subnet_2", emrClusterDefinition.getSubnetId());

        // Verify the external calls.
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testUpdateEmrClusterDefinitionWithAllValidInstanceFleetSubnets()
    {
        // Create an AWS params DTO
        AwsParamsDto awsParamsDto = new AwsParamsDto();

        // Create an EMR cluster alternate key DTO
        EmrClusterAlternateKeyDto emrClusterAlternateKeyDto = new EmrClusterAlternateKeyDto(NAMESPACE, EMR_CLUSTER_DEFINITION_NAME, EMR_CLUSTER_NAME);

        // Create an EMR cluster definition object
        EmrClusterDefinition emrClusterDefinition = new EmrClusterDefinition();
        emrClusterDefinition.setInstanceFleetMinimumIpAvailableFilter(1);
        emrClusterDefinition.setSubnetId("SUBNET_1,SUBNET_2,SUBNET_3");

        // Create subnet object
        List<Subnet> subnets = initializeTestSubnets(3);

        // Mock the external calls.
        when(emrPricingHelper.getSubnets(emrClusterDefinition, awsParamsDto)).thenReturn(subnets);
        when(jsonHelper.objectToJson(Mockito.any())).thenReturn("{jsonFormattedSubnetsAvailability}");

        // Call the method under test.
        emrHelperServiceImpl.updateEmrClusterDefinitionWithValidInstanceFleetSubnets(emrClusterAlternateKeyDto, emrClusterDefinition, awsParamsDto);

        // Verify correct value
        assertEquals("SUBNET_1,SUBNET_2,SUBNET_3", emrClusterDefinition.getSubnetId());

        // Verify the external calls.
        verify(emrPricingHelper).getSubnets(emrClusterDefinition, awsParamsDto);
        verify(jsonHelper).objectToJson(Mockito.any());
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testUpdateEmrClusterDefinitionWithOneValidInstanceFleetSubnet()
    {
        // Create an AWS params DTO
        AwsParamsDto awsParamsDto = new AwsParamsDto();

        // Create an EMR cluster alternate key DTO
        EmrClusterAlternateKeyDto emrClusterAlternateKeyDto = new EmrClusterAlternateKeyDto(NAMESPACE, EMR_CLUSTER_DEFINITION_NAME, EMR_CLUSTER_NAME);

        // Create an EMR cluster definition object
        EmrClusterDefinition emrClusterDefinition = new EmrClusterDefinition();
        emrClusterDefinition.setInstanceFleetMinimumIpAvailableFilter(25);
        emrClusterDefinition.setSubnetId("SUBNET_1,SUBNET_2,SUBNET_3");

        // Create subnet object
        List<Subnet> subnets = initializeTestSubnets(3);

        // Mock the external calls.
        when(emrPricingHelper.getSubnets(emrClusterDefinition, awsParamsDto)).thenReturn(subnets);
        when(jsonHelper.objectToJson(Mockito.any())).thenReturn("{jsonFormattedSubnetsAvailability}");

        // Call the method under test.
        emrHelperServiceImpl.updateEmrClusterDefinitionWithValidInstanceFleetSubnets(emrClusterAlternateKeyDto, emrClusterDefinition, awsParamsDto);

        // Verify correct value
        assertEquals("SUBNET_3", emrClusterDefinition.getSubnetId());

        // Verify the external calls.
        verify(emrPricingHelper).getSubnets(emrClusterDefinition, awsParamsDto);
        verify(jsonHelper).objectToJson(Mockito.any());
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testUpdateEmrClusterDefinitionWithNonValidInstanceFleetSubnets()
    {
        // Create an AWS params DTO
        AwsParamsDto awsParamsDto = new AwsParamsDto();

        // Create an EMR cluster alternate key DTO
        EmrClusterAlternateKeyDto emrClusterAlternateKeyDto = new EmrClusterAlternateKeyDto(NAMESPACE, EMR_CLUSTER_DEFINITION_NAME, EMR_CLUSTER_NAME);

        // Create an EMR cluster definition object
        EmrClusterDefinition emrClusterDefinition = new EmrClusterDefinition();
        emrClusterDefinition.setInstanceFleetMinimumIpAvailableFilter(50);

        // Create subnet object
        List<Subnet> subnets = initializeTestSubnets(3);

        // Mock the external calls.
        when(emrPricingHelper.getSubnets(emrClusterDefinition, awsParamsDto)).thenReturn(subnets);
        when(jsonHelper.objectToJson(Mockito.any())).thenReturn("{jsonFormattedSubnetsAvailability}");

        // Call the method under test.
        try
        {
            emrHelperServiceImpl.updateEmrClusterDefinitionWithValidInstanceFleetSubnets(emrClusterAlternateKeyDto, emrClusterDefinition, awsParamsDto);
            fail("IllegalArgumentException expected");
        }
        catch (IllegalArgumentException ex)
        {
            assertTrue(ex.getMessage().startsWith("Specified subnets do not have enough available IP addresses required for the instance fleet."));
            assertTrue(ex.getMessage().contains(NAMESPACE));
            assertTrue(ex.getMessage().contains(EMR_CLUSTER_DEFINITION_NAME));
            assertTrue(ex.getMessage().contains(EMR_CLUSTER_NAME));
            assertTrue(ex.getMessage().contains("{jsonFormattedSubnetsAvailability}"));
        }

        // Verify the external calls.
        verify(emrPricingHelper).getSubnets(emrClusterDefinition, awsParamsDto);
        verify(jsonHelper).objectToJson(Mockito.any());
        verifyNoMoreInteractionsHelper();

    }

    private List<Subnet> initializeTestSubnets(int n)
    {
        List<Subnet> subnets = new ArrayList<>();
        for (int i = 1; i <= n; i++)
        {
            Subnet subnet = new Subnet();
            subnet.setSubnetId("SUBNET_" + i);
            subnet.setAvailableIpAddressCount(i * 10);
            subnets.add(subnet);
        }

        return subnets;
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
                "masterSecurityGroup", "slaveSecurityGroup", "serviceAccessSecurityGroup", "scaleDownBehavior", new EmrClusterDefinitionKerberosAttributes(),
                1, new EmrClusterDefinitionAutoTerminationPolicy());

        EmrClusterDefinition emrClusterDefinitionOverride =
            new EmrClusterDefinition("sshKeyPairNameOverride", "subnetIdOverride", "logBucketOverride", false, false, false, false, "accountIdOverride",
                "serviceIamRoleOverride", "ec2NodeIamProfileNameOverride", "amiVersionOverride", "releaseLabelOverride", "hadoopVersionOverride",
                "hiveVersionOverride", "pigVersionOverride", false, Lists.newArrayList(new ScriptDefinition(), new ScriptDefinition()),
                Lists.newArrayList(new ScriptDefinition(), new ScriptDefinition()), "additionalInfoOverride", new InstanceDefinitions(), 10,
                Lists.newArrayList(new EmrClusterDefinitionInstanceFleet(), new EmrClusterDefinitionInstanceFleet()),
                Lists.newArrayList(new NodeTag(), new NodeTag()), "supportedProductOverride",
                Lists.newArrayList(new EmrClusterDefinitionApplication(), new EmrClusterDefinitionApplication()),
                Lists.newArrayList(new EmrClusterDefinitionConfiguration(), new EmrClusterDefinitionConfiguration()),
                Lists.newArrayList(new Parameter(), new Parameter()), Lists.newArrayList(new Byte("0"), new Byte("0")),
                Lists.newArrayList(new HadoopJarStep(), new HadoopJarStep()),
                Lists.newArrayList("additionalMasterSecurityGroupsOverride", "additionalMasterSecurityGroupsOverride"),
                Lists.newArrayList("additionalSlaveSecurityGroupsOverride", "additionalSlaveSecurityGroupsOverride"), "securityConfigurationOverride",
                "masterSecurityGroupOverride", "slaveSecurityGroupOverride", "serviceSecurityGroupOverride", "scaleDownBehaviorOverride",
                new EmrClusterDefinitionKerberosAttributes(), 1, new EmrClusterDefinitionAutoTerminationPolicy());

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
                "masterSecurityGroup", "slaveSecurityGroup", "serviceAccessSecurityGroup", "scaleDownBehavior", new EmrClusterDefinitionKerberosAttributes(),
                1, new EmrClusterDefinitionAutoTerminationPolicy());

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
            emrPricingHelper, herdDao, namespaceDaoHelper, namespaceIamRoleAuthorizationHelper, xmlHelper, jsonHelper);
    }
}
