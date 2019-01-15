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

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.elasticmapreduce.model.ClusterSummary;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.EmrDao;
import org.finra.herd.dao.HerdDao;
import org.finra.herd.dao.helper.EmrHelper;
import org.finra.herd.dao.helper.EmrPricingHelper;
import org.finra.herd.dao.helper.XmlHelper;
import org.finra.herd.model.api.xml.EmrClusterCreateRequest;
import org.finra.herd.model.api.xml.EmrClusterDefinition;
import org.finra.herd.model.api.xml.EmrClusterDefinitionKey;
import org.finra.herd.model.dto.AwsParamsDto;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.EmrClusterAlternateKeyDto;
import org.finra.herd.model.dto.EmrClusterCreateDto;
import org.finra.herd.model.jpa.EmrClusterCreationLogEntity;
import org.finra.herd.model.jpa.EmrClusterDefinitionEntity;
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.service.EmrHelperService;
import org.finra.herd.service.helper.AwsHelper;
import org.finra.herd.service.helper.EmrClusterDefinitionDaoHelper;
import org.finra.herd.service.helper.EmrClusterDefinitionHelper;
import org.finra.herd.service.helper.NamespaceIamRoleAuthorizationHelper;

/**
 * EmrHelperServiceImpl
 */
@Service
public class EmrHelperServiceImpl implements EmrHelperService
{
    @Autowired
    private AwsHelper awsHelper;

    @Autowired
    private ConfigurationHelper configurationHelper;

    @Autowired
    private EmrClusterDefinitionDaoHelper emrClusterDefinitionDaoHelper;

    @Autowired
    private EmrClusterDefinitionHelper emrClusterDefinitionHelper;

    @Autowired
    private EmrDao emrDao;

    @Autowired
    private EmrHelper emrHelper;

    @Autowired
    private EmrPricingHelper emrPricingHelper;

    @Autowired
    private HerdDao herdDao;

    @Autowired
    private NamespaceIamRoleAuthorizationHelper namespaceIamRoleAuthorizationHelper;

    @Autowired
    private XmlHelper xmlHelper;

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation executes non-transactionally, suspends the current transaction if one exists.
     */
    @Override
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    public EmrClusterCreateDto emrCreateClusterAwsSpecificSteps(EmrClusterCreateRequest request, EmrClusterDefinition emrClusterDefinition,
        EmrClusterAlternateKeyDto emrClusterAlternateKeyDto)
    {
        return emrCreateClusterAwsSpecificStepsImpl(request, emrClusterDefinition, emrClusterAlternateKeyDto);
    }

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation starts a new transaction.
     */
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public EmrClusterDefinition emrPreCreateClusterSteps(EmrClusterAlternateKeyDto emrClusterAlternateKeyDto, EmrClusterCreateRequest request) throws Exception
    {
        return emrPreCreateClusterStepsImpl(emrClusterAlternateKeyDto, request);
    }

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation starts a new transaction.
     */
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void logEmrClusterCreation(EmrClusterAlternateKeyDto emrClusterAlternateKeyDto, EmrClusterDefinition emrClusterDefinition, String clusterId)
        throws Exception
    {
        logEmrClusterCreationImpl(emrClusterAlternateKeyDto, emrClusterDefinition, clusterId);
    }

    EmrClusterCreateDto emrCreateClusterAwsSpecificStepsImpl(EmrClusterCreateRequest request, EmrClusterDefinition emrClusterDefinition,
        EmrClusterAlternateKeyDto emrClusterAlternateKeyDto)
    {
        AwsParamsDto awsParamsDto = emrHelper.getAwsParamsDtoByAcccountId(emrClusterDefinition.getAccountId());

        // If instance group definitions are specified, find best price and update definition.
        if (!emrHelper.isInstanceDefinitionsEmpty(emrClusterDefinition.getInstanceDefinitions()))
        {
            emrPricingHelper.updateEmrClusterDefinitionWithBestPrice(emrClusterAlternateKeyDto, emrClusterDefinition, awsParamsDto);
        }

        // The cluster ID record.
        String clusterId = null;

        // Is EMR cluster created.
        Boolean emrClusterCreated = null;

        // EMR cluster status string.
        String emrClusterStatus = null;

        // If the dryRun flag is null or false. This is the default option if no flag is given.
        if (!Boolean.TRUE.equals(request.isDryRun()))
        {
            /*
             * Create the cluster only if the cluster does not already exist.
             * If the cluster is created, record the newly created cluster ID.
             * If the cluster already exists, record the existing cluster ID.
             * If there is any error while attempting to check for existing cluster or create a new one, handle the exception to throw appropriate exception.
             */
            String clusterName = emrHelper
                .buildEmrClusterName(emrClusterAlternateKeyDto.getNamespace(), emrClusterAlternateKeyDto.getEmrClusterDefinitionName(),
                    emrClusterAlternateKeyDto.getEmrClusterName());
            try
            {
                // Try to get an active EMR cluster by its name.
                ClusterSummary clusterSummary = emrDao.getActiveEmrClusterByName(clusterName, awsParamsDto);

                // If cluster does not already exist.
                if (clusterSummary == null)
                {
                    clusterId = emrDao.createEmrCluster(clusterName, emrClusterDefinition, awsParamsDto);
                    emrClusterCreated = true;
                }
                // If the cluster already exists.
                else
                {
                    clusterId = clusterSummary.getId();
                    emrClusterCreated = false;
                }

                emrClusterStatus = emrDao.getEmrClusterStatusById(clusterId, awsParamsDto);
            }
            catch (AmazonServiceException ex)
            {
                awsHelper.handleAmazonException(ex, "An Amazon exception occurred while creating EMR cluster with name \"" + clusterName + "\".");
            }
        }
        // If the dryRun flag is true and not null
        else
        {
            emrClusterCreated = false;
        }

        return new EmrClusterCreateDto(clusterId, emrClusterCreated, emrClusterStatus);
    }

    EmrClusterDefinition emrPreCreateClusterStepsImpl(EmrClusterAlternateKeyDto emrClusterAlternateKeyDto, EmrClusterCreateRequest request) throws Exception
    {
        // Get the EMR cluster definition and ensure it exists.
        EmrClusterDefinitionEntity emrClusterDefinitionEntity = emrClusterDefinitionDaoHelper.getEmrClusterDefinitionEntity(
            new EmrClusterDefinitionKey(emrClusterAlternateKeyDto.getNamespace(), emrClusterAlternateKeyDto.getEmrClusterDefinitionName()));

        // Replace all S3 managed location variables in xml
        String toReplace = getS3ManagedReplaceString();
        String replacedConfigXml = emrClusterDefinitionEntity.getConfiguration().replaceAll(toReplace, emrHelper.getS3StagingLocation());

        // Unmarshal definition xml into JAXB object.
        EmrClusterDefinition emrClusterDefinition = xmlHelper.unmarshallXmlToObject(EmrClusterDefinition.class, replacedConfigXml);

        // Perform override if override is set.
        overrideEmrClusterDefinition(emrClusterDefinition, request.getEmrClusterDefinitionOverride());

        // Perform the EMR cluster definition configuration validation.
        emrClusterDefinitionHelper.validateEmrClusterDefinitionConfiguration(emrClusterDefinition);

        // Check permissions.
        namespaceIamRoleAuthorizationHelper.checkPermissions(emrClusterDefinitionEntity.getNamespace(), emrClusterDefinition.getServiceIamRole(),
            emrClusterDefinition.getEc2NodeIamProfileName());

        return emrClusterDefinition;
    }

    void logEmrClusterCreationImpl(EmrClusterAlternateKeyDto emrClusterAlternateKeyDto, EmrClusterDefinition emrClusterDefinition, String clusterId)
        throws Exception
    {
        NamespaceEntity namespaceEntity = new NamespaceEntity();
        namespaceEntity.setCode(emrClusterAlternateKeyDto.getNamespace());
        EmrClusterCreationLogEntity emrClusterCreationLogEntity = new EmrClusterCreationLogEntity();
        emrClusterCreationLogEntity.setNamespace(namespaceEntity);
        emrClusterCreationLogEntity.setEmrClusterDefinitionName(emrClusterAlternateKeyDto.getEmrClusterDefinitionName());
        emrClusterCreationLogEntity.setEmrClusterName(emrClusterAlternateKeyDto.getEmrClusterName());
        emrClusterCreationLogEntity.setEmrClusterId(clusterId);
        emrClusterCreationLogEntity.setEmrClusterDefinition(xmlHelper.objectToXml(emrClusterDefinition));
        herdDao.saveAndRefresh(emrClusterCreationLogEntity);
    }

    private String getS3ManagedReplaceString()
    {
        return configurationHelper.getProperty(ConfigurationValue.S3_STAGING_RESOURCE_LOCATION);
    }

    /**
     * <p> Overrides the properties of {@code emrClusterDefinition} with the properties of {@code emrClusterDefinitionOverride}. </p> <p> If any property in
     * {@code emrClusterDefinitionOverride} is {@code null}, the property will remain unmodified. </p> <p> If any property in {@code
     * emrClusterDefinitionOverride} is not {@code null}, the property will be set. </p> <p> Any list or object type properties will be shallowly overridden.
     * That is, if a list is given in the override, the entire list will be set. Note that this is a shallow copy operation, so any modification to the override
     * list or object will affect the definition. </p> <p> This method does nothing if {@code emrClusterDefinitionOverride} is {@code null}. </p>
     *
     * @param emrClusterDefinition - definition to override
     * @param emrClusterDefinitionOverride - the override value or {@code null}
     */
    @SuppressWarnings("PMD.CyclomaticComplexity") // Method is not complex. It's just very repetitive.
    private void overrideEmrClusterDefinition(EmrClusterDefinition emrClusterDefinition, EmrClusterDefinition emrClusterDefinitionOverride)
    {
        if (emrClusterDefinitionOverride != null)
        {
            if (emrClusterDefinitionOverride.getReleaseLabel() != null)
            {
                emrClusterDefinition.setReleaseLabel(emrClusterDefinitionOverride.getReleaseLabel());
            }
            if (emrClusterDefinitionOverride.getApplications() != null)
            {
                emrClusterDefinition.setApplications(emrClusterDefinitionOverride.getApplications());
            }
            if (emrClusterDefinitionOverride.getConfigurations() != null)
            {
                emrClusterDefinition.setConfigurations(emrClusterDefinitionOverride.getConfigurations());
            }
            if (emrClusterDefinitionOverride.getSshKeyPairName() != null)
            {
                emrClusterDefinition.setSshKeyPairName(emrClusterDefinitionOverride.getSshKeyPairName());
            }
            if (emrClusterDefinitionOverride.getSubnetId() != null)
            {
                emrClusterDefinition.setSubnetId(emrClusterDefinitionOverride.getSubnetId());
            }
            if (emrClusterDefinitionOverride.getLogBucket() != null)
            {
                emrClusterDefinition.setLogBucket(emrClusterDefinitionOverride.getLogBucket());
            }
            if (emrClusterDefinitionOverride.isKeepAlive() != null)
            {
                emrClusterDefinition.setKeepAlive(emrClusterDefinitionOverride.isKeepAlive());
            }
            if (emrClusterDefinitionOverride.isVisibleToAll() != null)
            {
                emrClusterDefinition.setVisibleToAll(emrClusterDefinitionOverride.isVisibleToAll());
            }
            if (emrClusterDefinitionOverride.isTerminationProtection() != null)
            {
                emrClusterDefinition.setTerminationProtection(emrClusterDefinitionOverride.isTerminationProtection());
            }
            if (emrClusterDefinitionOverride.isEncryptionEnabled() != null)
            {
                emrClusterDefinition.setEncryptionEnabled(emrClusterDefinitionOverride.isEncryptionEnabled());
            }
            if (emrClusterDefinitionOverride.getAmiVersion() != null)
            {
                emrClusterDefinition.setAmiVersion(emrClusterDefinitionOverride.getAmiVersion());
            }
            if (emrClusterDefinitionOverride.getHadoopVersion() != null)
            {
                emrClusterDefinition.setHadoopVersion(emrClusterDefinitionOverride.getHadoopVersion());
            }
            if (emrClusterDefinitionOverride.getHiveVersion() != null)
            {
                emrClusterDefinition.setHiveVersion(emrClusterDefinitionOverride.getHiveVersion());
            }
            if (emrClusterDefinitionOverride.getPigVersion() != null)
            {
                emrClusterDefinition.setPigVersion(emrClusterDefinitionOverride.getPigVersion());
            }
            if (emrClusterDefinitionOverride.getEc2NodeIamProfileName() != null)
            {
                emrClusterDefinition.setEc2NodeIamProfileName(emrClusterDefinitionOverride.getEc2NodeIamProfileName());
            }
            if (emrClusterDefinitionOverride.isInstallOozie() != null)
            {
                emrClusterDefinition.setInstallOozie(emrClusterDefinitionOverride.isInstallOozie());
            }
            if (emrClusterDefinitionOverride.getCustomBootstrapActionMaster() != null)
            {
                emrClusterDefinition.setCustomBootstrapActionMaster(emrClusterDefinitionOverride.getCustomBootstrapActionMaster());
            }
            if (emrClusterDefinitionOverride.getCustomBootstrapActionAll() != null)
            {
                emrClusterDefinition.setCustomBootstrapActionAll(emrClusterDefinitionOverride.getCustomBootstrapActionAll());
            }
            if (emrClusterDefinitionOverride.getAdditionalInfo() != null)
            {
                emrClusterDefinition.setAdditionalInfo(emrClusterDefinitionOverride.getAdditionalInfo());
            }
            if (emrClusterDefinitionOverride.getInstanceDefinitions() != null)
            {
                emrClusterDefinition.setInstanceDefinitions(emrClusterDefinitionOverride.getInstanceDefinitions());
            }
            if (emrClusterDefinitionOverride.getInstanceFleets() != null)
            {
                emrClusterDefinition.setInstanceFleets(emrClusterDefinitionOverride.getInstanceFleets());
            }
            if (emrClusterDefinitionOverride.getNodeTags() != null)
            {
                emrClusterDefinition.setNodeTags(emrClusterDefinitionOverride.getNodeTags());
            }
            if (emrClusterDefinitionOverride.getDaemonConfigurations() != null)
            {
                emrClusterDefinition.setDaemonConfigurations(emrClusterDefinitionOverride.getDaemonConfigurations());
            }
            if (emrClusterDefinitionOverride.getHadoopConfigurations() != null)
            {
                emrClusterDefinition.setHadoopConfigurations(emrClusterDefinitionOverride.getHadoopConfigurations());
            }
            if (emrClusterDefinitionOverride.getAccountId() != null)
            {
                emrClusterDefinition.setAccountId(emrClusterDefinitionOverride.getAccountId());
            }
            if (emrClusterDefinitionOverride.getServiceIamRole() != null)
            {
                emrClusterDefinition.setServiceIamRole(emrClusterDefinitionOverride.getServiceIamRole());
            }
            if (emrClusterDefinitionOverride.getSupportedProduct() != null)
            {
                emrClusterDefinition.setSupportedProduct(emrClusterDefinitionOverride.getSupportedProduct());
            }
            if (emrClusterDefinitionOverride.getHadoopJarSteps() != null)
            {
                emrClusterDefinition.setHadoopJarSteps(emrClusterDefinitionOverride.getHadoopJarSteps());
            }
            if (emrClusterDefinitionOverride.getAdditionalMasterSecurityGroups() != null)
            {
                emrClusterDefinition.setAdditionalMasterSecurityGroups(emrClusterDefinitionOverride.getAdditionalMasterSecurityGroups());
            }
            if (emrClusterDefinitionOverride.getAdditionalSlaveSecurityGroups() != null)
            {
                emrClusterDefinition.setAdditionalSlaveSecurityGroups(emrClusterDefinitionOverride.getAdditionalSlaveSecurityGroups());
            }
            if (emrClusterDefinitionOverride.getSecurityConfiguration() != null)
            {
                emrClusterDefinition.setSecurityConfiguration(emrClusterDefinitionOverride.getSecurityConfiguration());
            }
            if (emrClusterDefinitionOverride.getMasterSecurityGroup() != null)
            {
                emrClusterDefinition.setMasterSecurityGroup(emrClusterDefinitionOverride.getMasterSecurityGroup());
            }
            if (emrClusterDefinitionOverride.getSlaveSecurityGroup() != null)
            {
                emrClusterDefinition.setSlaveSecurityGroup(emrClusterDefinitionOverride.getSlaveSecurityGroup());
            }
            if (emrClusterDefinitionOverride.getScaleDownBehavior() != null)
            {
                emrClusterDefinition.setScaleDownBehavior(emrClusterDefinitionOverride.getScaleDownBehavior());
            }
            if (emrClusterDefinitionOverride.getKerberosAttributes() != null)
            {
                emrClusterDefinition.setKerberosAttributes(emrClusterDefinitionOverride.getKerberosAttributes());
            }
        }
    }
}
