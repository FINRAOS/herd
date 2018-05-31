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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import org.finra.herd.dao.EmrClusterDefinitionDao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.dao.helper.XmlHelper;
import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.annotation.NamespacePermission;
import org.finra.herd.model.api.xml.EmrClusterDefinition;
import org.finra.herd.model.api.xml.EmrClusterDefinitionCreateRequest;
import org.finra.herd.model.api.xml.EmrClusterDefinitionInformation;
import org.finra.herd.model.api.xml.EmrClusterDefinitionKey;
import org.finra.herd.model.api.xml.EmrClusterDefinitionKeys;
import org.finra.herd.model.api.xml.EmrClusterDefinitionUpdateRequest;
import org.finra.herd.model.api.xml.NamespacePermissionEnum;
import org.finra.herd.model.jpa.EmrClusterDefinitionEntity;
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.service.EmrClusterDefinitionService;
import org.finra.herd.service.helper.EmrClusterDefinitionDaoHelper;
import org.finra.herd.service.helper.EmrClusterDefinitionHelper;
import org.finra.herd.service.helper.NamespaceDaoHelper;
import org.finra.herd.service.helper.NamespaceIamRoleAuthorizationHelper;

/**
 * The EMR cluster definition service implementation.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
@SuppressFBWarnings(value = "VA_FORMAT_STRING_USES_NEWLINE", justification = "We will use the standard carriage return character.")
public class EmrClusterDefinitionServiceImpl implements EmrClusterDefinitionService
{
    private static final Logger LOGGER = LoggerFactory.getLogger(EmrClusterDefinitionServiceImpl.class);

    @Autowired
    private EmrClusterDefinitionDao emrClusterDefinitionDao;

    @Autowired
    private EmrClusterDefinitionDaoHelper emrClusterDefinitionDaoHelper;

    @Autowired
    private EmrClusterDefinitionHelper emrClusterDefinitionHelper;

    @Autowired
    private NamespaceDaoHelper namespaceDaoHelper;

    @Autowired
    private NamespaceIamRoleAuthorizationHelper namespaceIamRoleAuthorizationHelper;

    @Autowired
    protected XmlHelper xmlHelper;

    @NamespacePermission(fields = "#request?.emrClusterDefinitionKey?.namespace", permissions = NamespacePermissionEnum.WRITE)
    @Override
    public EmrClusterDefinitionInformation createEmrClusterDefinition(EmrClusterDefinitionCreateRequest request) throws Exception
    {
        // Perform validate and trim of the EMR cluster definition key.
        emrClusterDefinitionHelper.validateEmrClusterDefinitionKey(request.getEmrClusterDefinitionKey());

        // Validate the EMR cluster definition configuration.
        emrClusterDefinitionHelper.validateEmrClusterDefinitionConfiguration(request.getEmrClusterDefinition());

        // Get the namespace and ensure it exists.
        NamespaceEntity namespaceEntity = namespaceDaoHelper.getNamespaceEntity(request.getEmrClusterDefinitionKey().getNamespace());

        namespaceIamRoleAuthorizationHelper.checkPermissions(namespaceEntity, request.getEmrClusterDefinition().getServiceIamRole(),
            request.getEmrClusterDefinition().getEc2NodeIamProfileName());

        // Ensure a EMR cluster definition with the specified name doesn't already exist.
        EmrClusterDefinitionEntity emrClusterDefinitionEntity = emrClusterDefinitionDao
            .getEmrClusterDefinitionByNamespaceAndName(namespaceEntity, request.getEmrClusterDefinitionKey().getEmrClusterDefinitionName());
        if (emrClusterDefinitionEntity != null)
        {
            throw new AlreadyExistsException(String
                .format("Unable to create EMR cluster definition with name \"%s\" for namespace \"%s\" because it already exists.",
                    request.getEmrClusterDefinitionKey().getEmrClusterDefinitionName(), request.getEmrClusterDefinitionKey().getNamespace()));
        }

        // Create a EMR cluster definition entity from the request information.
        emrClusterDefinitionEntity = createEmrClusterDefinitionEntity(namespaceEntity, request);

        // Persist the new entity.
        emrClusterDefinitionEntity = emrClusterDefinitionDao.saveAndRefresh(emrClusterDefinitionEntity);

        // Create and return the EMR cluster definition object from the persisted entity.
        return createEmrClusterDefinitionFromEntity(emrClusterDefinitionEntity);
    }

    @NamespacePermission(fields = "#emrClusterDefinitionKey?.namespace", permissions = NamespacePermissionEnum.READ)
    @Override
    public EmrClusterDefinitionInformation getEmrClusterDefinition(EmrClusterDefinitionKey emrClusterDefinitionKey) throws Exception
    {
        // Perform validate and trim of the EMR cluster definition key.
        emrClusterDefinitionHelper.validateEmrClusterDefinitionKey(emrClusterDefinitionKey);

        // Retrieve and ensure that a EMR cluster definition exists with the specified key.
        EmrClusterDefinitionEntity emrClusterDefinitionEntity = emrClusterDefinitionDaoHelper.getEmrClusterDefinitionEntity(emrClusterDefinitionKey);

        // Create and return the EMR cluster definition object from the persisted entity.
        return createEmrClusterDefinitionFromEntity(emrClusterDefinitionEntity);
    }

    @NamespacePermission(fields = "#emrClusterDefinitionKey?.namespace", permissions = NamespacePermissionEnum.WRITE)
    @Override
    public EmrClusterDefinitionInformation updateEmrClusterDefinition(EmrClusterDefinitionKey emrClusterDefinitionKey,
        EmrClusterDefinitionUpdateRequest request) throws Exception
    {
        // Perform validate and trim of the EMR cluster definition key.
        emrClusterDefinitionHelper.validateEmrClusterDefinitionKey(emrClusterDefinitionKey);

        // Validate the EMR cluster definition configuration.
        emrClusterDefinitionHelper.validateEmrClusterDefinitionConfiguration(request.getEmrClusterDefinition());

        // Retrieve and ensure that a EMR cluster definition already exists with the specified name.
        EmrClusterDefinitionEntity emrClusterDefinitionEntity = emrClusterDefinitionDaoHelper.getEmrClusterDefinitionEntity(emrClusterDefinitionKey);

        namespaceIamRoleAuthorizationHelper.checkPermissions(emrClusterDefinitionEntity.getNamespace(), request.getEmrClusterDefinition().getServiceIamRole(),
            request.getEmrClusterDefinition().getEc2NodeIamProfileName());

        // Log the existing EMR cluster definition before the update.
        LOGGER.info("Logging EMR cluster definition before the update. emrClusterDefinition={}",
            xmlHelper.objectToXml(createEmrClusterDefinitionFromEntity(emrClusterDefinitionEntity), true));

        // Convert EMR cluster configuration to the XML representation.
        String emrClusterConfiguration = xmlHelper.objectToXml(request.getEmrClusterDefinition());

        // Update the EMR cluster definition entity.
        emrClusterDefinitionEntity.setConfiguration(emrClusterConfiguration);

        // Persist and refresh the entity.
        emrClusterDefinitionEntity = emrClusterDefinitionDao.saveAndRefresh(emrClusterDefinitionEntity);

        // Create and return the EMR cluster definition object from the persisted entity.
        return createEmrClusterDefinitionFromEntity(emrClusterDefinitionEntity);
    }

    @NamespacePermission(fields = "#emrClusterDefinitionKey?.namespace", permissions = NamespacePermissionEnum.WRITE)
    @Override
    public EmrClusterDefinitionInformation deleteEmrClusterDefinition(EmrClusterDefinitionKey emrClusterDefinitionKey) throws Exception
    {
        // Perform validate and trim of the EMR cluster definition key.
        emrClusterDefinitionHelper.validateEmrClusterDefinitionKey(emrClusterDefinitionKey);

        // Retrieve and ensure that a EMR cluster definition already exists with the specified key.
        EmrClusterDefinitionEntity emrClusterDefinitionEntity = emrClusterDefinitionDaoHelper.getEmrClusterDefinitionEntity(emrClusterDefinitionKey);

        // Log the existing EMR cluster definition.
        LOGGER.info("Logging EMR cluster definition being deleted. emrClusterDefinition={}",
            xmlHelper.objectToXml(createEmrClusterDefinitionFromEntity(emrClusterDefinitionEntity), true));

        // Delete the EMR cluster definition.
        emrClusterDefinitionDao.delete(emrClusterDefinitionEntity);

        // Create and return the EMR cluster definition object from the deleted entity.
        return createEmrClusterDefinitionFromEntity(emrClusterDefinitionEntity);
    }

    @NamespacePermission(fields = "#namespace", permissions = NamespacePermissionEnum.READ)
    @Override
    public EmrClusterDefinitionKeys getEmrClusterDefinitions(String namespace)
    {
        // Validate the namespace.
        Assert.hasText(namespace, "A namespace must be specified.");

        // Retrieve and return the list of EMR cluster definition keys.
        EmrClusterDefinitionKeys emrClusterDefinitionKeys = new EmrClusterDefinitionKeys();
        emrClusterDefinitionKeys.getEmrClusterDefinitionKeys().addAll(emrClusterDefinitionDaoHelper.getEmrClusterDefinitionKeys(namespace.trim()));
        return emrClusterDefinitionKeys;
    }

    /**
     * Creates a new EMR cluster definition entity from the request information.
     *
     * @param namespaceEntity the namespace entity
     * @param request the EMR cluster definition create request
     *
     * @return the newly created EMR cluster definition entity
     */
    private EmrClusterDefinitionEntity createEmrClusterDefinitionEntity(NamespaceEntity namespaceEntity, EmrClusterDefinitionCreateRequest request)
        throws Exception
    {
        // Convert EMR cluster configuration to the XML representation.
        String emrClusterConfiguration = xmlHelper.objectToXml(request.getEmrClusterDefinition());

        // Create a new entity.
        EmrClusterDefinitionEntity emrClusterDefinitionEntity = new EmrClusterDefinitionEntity();

        emrClusterDefinitionEntity.setNamespace(namespaceEntity);
        emrClusterDefinitionEntity.setName(request.getEmrClusterDefinitionKey().getEmrClusterDefinitionName());
        emrClusterDefinitionEntity.setConfiguration(emrClusterConfiguration);

        return emrClusterDefinitionEntity;
    }

    /**
     * Creates the EMR cluster definition information from the persisted entity.
     *
     * @param emrClusterDefinitionEntity the EMR cluster definition entity
     *
     * @return the EMR cluster definition information
     */
    private EmrClusterDefinitionInformation createEmrClusterDefinitionFromEntity(EmrClusterDefinitionEntity emrClusterDefinitionEntity) throws Exception
    {
        // Unmarshal EMR cluster definition XML into JAXB object.
        EmrClusterDefinition emrClusterDefinition = xmlHelper.unmarshallXmlToObject(EmrClusterDefinition.class, emrClusterDefinitionEntity.getConfiguration());

        // Create a new instance of EMR cluster definition information.
        EmrClusterDefinitionInformation emrClusterDefinitionInformation = new EmrClusterDefinitionInformation();

        emrClusterDefinitionInformation.setId(emrClusterDefinitionEntity.getId());
        emrClusterDefinitionInformation
            .setEmrClusterDefinitionKey(new EmrClusterDefinitionKey(emrClusterDefinitionEntity.getNamespace().getCode(), emrClusterDefinitionEntity.getName()));
        emrClusterDefinitionInformation.setEmrClusterDefinition(emrClusterDefinition);

        return emrClusterDefinitionInformation;
    }
}
