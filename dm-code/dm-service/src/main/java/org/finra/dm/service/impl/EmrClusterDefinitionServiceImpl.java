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
package org.finra.dm.service.impl;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import org.finra.dm.dao.DmDao;
import org.finra.dm.dao.config.DaoSpringModuleConfig;
import org.finra.dm.model.AlreadyExistsException;
import org.finra.dm.model.jpa.EmrClusterDefinitionEntity;
import org.finra.dm.model.jpa.NamespaceEntity;
import org.finra.dm.model.api.xml.EmrClusterDefinition;
import org.finra.dm.model.api.xml.EmrClusterDefinitionCreateRequest;
import org.finra.dm.model.api.xml.EmrClusterDefinitionInformation;
import org.finra.dm.model.api.xml.EmrClusterDefinitionKey;
import org.finra.dm.model.api.xml.EmrClusterDefinitionUpdateRequest;
import org.finra.dm.service.EmrClusterDefinitionService;
import org.finra.dm.service.helper.DmDaoHelper;
import org.finra.dm.service.helper.DmHelper;
import org.finra.dm.dao.helper.XmlHelper;

/**
 * The EMR cluster definition service implementation.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.DM_TRANSACTION_MANAGER_BEAN_NAME)
@SuppressFBWarnings(value = "VA_FORMAT_STRING_USES_NEWLINE", justification = "We will use the standard carriage return character.")
public class EmrClusterDefinitionServiceImpl implements EmrClusterDefinitionService
{
    private static final Logger LOGGER = Logger.getLogger(EmrClusterDefinitionServiceImpl.class);

    @Autowired
    private DmHelper dmHelper;

    @Autowired
    protected XmlHelper xmlHelper;

    @Autowired
    private DmDao dmDao;

    @Autowired
    private DmDaoHelper dmDaoHelper;

    /**
     * Creates a new EMR cluster definition.
     *
     * @param request the information needed to create an EMR cluster definition
     *
     * @return the newly created EMR cluster definition
     */
    @Override
    public EmrClusterDefinitionInformation createEmrClusterDefinition(EmrClusterDefinitionCreateRequest request) throws Exception
    {
        // Perform validate and trim of the EMR cluster definition key.
        dmHelper.validateEmrClusterDefinitionKey(request.getEmrClusterDefinitionKey());

        // Validate the EMR cluster definition configuration.
        dmHelper.validateEmrClusterDefinitionConfiguration(request.getEmrClusterDefinition());

        // Get the namespace and ensure it exists.
        NamespaceEntity namespaceEntity = dmDaoHelper.getNamespaceEntity(request.getEmrClusterDefinitionKey().getNamespace());

        // Ensure a EMR cluster definition with the specified name doesn't already exist.
        EmrClusterDefinitionEntity emrClusterDefinitionEntity = dmDao.getEmrClusterDefinitionByAltKey(request.getEmrClusterDefinitionKey());
        if (emrClusterDefinitionEntity != null)
        {
            throw new AlreadyExistsException(String
                .format("Unable to create EMR cluster definition with name \"%s\" for namespace \"%s\" because it already exists.",
                    request.getEmrClusterDefinitionKey().getEmrClusterDefinitionName(), request.getEmrClusterDefinitionKey().getNamespace()));
        }

        // Create a EMR cluster definition entity from the request information.
        emrClusterDefinitionEntity = createEmrClusterDefinitionEntity(namespaceEntity, request);

        // Persist the new entity.
        emrClusterDefinitionEntity = dmDao.saveAndRefresh(emrClusterDefinitionEntity);

        // Create and return the EMR cluster definition object from the persisted entity.
        return createEmrClusterDefinitionFromEntity(emrClusterDefinitionEntity);
    }

    /**
     * Gets an existing EMR cluster definition by key.
     *
     * @param emrClusterDefinitionKey the EMR cluster definition key
     *
     * @return the EMR cluster definition
     */
    @Override
    public EmrClusterDefinitionInformation getEmrClusterDefinition(EmrClusterDefinitionKey emrClusterDefinitionKey) throws Exception
    {
        // Perform validate and trim of the EMR cluster definition key.
        dmHelper.validateEmrClusterDefinitionKey(emrClusterDefinitionKey);

        // Retrieve and ensure that a EMR cluster definition exists with the specified key.
        EmrClusterDefinitionEntity emrClusterDefinitionEntity = dmDaoHelper.getEmrClusterDefinitionEntity(emrClusterDefinitionKey);

        // Create and return the EMR cluster definition object from the persisted entity.
        return createEmrClusterDefinitionFromEntity(emrClusterDefinitionEntity);
    }

    /**
     * Updates an existing EMR cluster definition.
     *
     * @param emrClusterDefinitionKey the EMR cluster definition key
     * @param request the information needed to update the EMR cluster definition
     *
     * @return the updated EMR cluster definition
     */
    @Override
    public EmrClusterDefinitionInformation updateEmrClusterDefinition(EmrClusterDefinitionKey emrClusterDefinitionKey,
        EmrClusterDefinitionUpdateRequest request) throws Exception
    {
        // Perform validate and trim of the EMR cluster definition key.
        dmHelper.validateEmrClusterDefinitionKey(emrClusterDefinitionKey);

        // Validate the EMR cluster definition configuration.
        dmHelper.validateEmrClusterDefinitionConfiguration(request.getEmrClusterDefinition());

        // Retrieve and ensure that a EMR cluster definition already exists with the specified name.
        EmrClusterDefinitionEntity emrClusterDefinitionEntity = dmDaoHelper.getEmrClusterDefinitionEntity(emrClusterDefinitionKey);

        // Log the existing EMR cluster definition before the update.
        LOGGER.info(String.format("EMR cluster definition before the update:\n%s",
            xmlHelper.objectToXml(createEmrClusterDefinitionFromEntity(emrClusterDefinitionEntity), true)));

        // Convert EMR cluster configuration to the XML representation.
        String emrClusterConfiguration = xmlHelper.objectToXml(request.getEmrClusterDefinition());

        // Update the EMR cluster definition entity.
        emrClusterDefinitionEntity.setConfiguration(emrClusterConfiguration);

        // Persist and refresh the entity.
        emrClusterDefinitionEntity = dmDao.saveAndRefresh(emrClusterDefinitionEntity);

        // Create and return the EMR cluster definition object from the persisted entity.
        return createEmrClusterDefinitionFromEntity(emrClusterDefinitionEntity);
    }

    /**
     * Deletes an existing EMR cluster definition by key.
     *
     * @param emrClusterDefinitionKey the EMR cluster definition key
     *
     * @return the EMR cluster definition that got deleted
     */
    @Override
    public EmrClusterDefinitionInformation deleteEmrClusterDefinition(EmrClusterDefinitionKey emrClusterDefinitionKey) throws Exception
    {
        // Perform validate and trim of the EMR cluster definition key.
        dmHelper.validateEmrClusterDefinitionKey(emrClusterDefinitionKey);

        // Retrieve and ensure that a EMR cluster definition already exists with the specified key.
        EmrClusterDefinitionEntity emrClusterDefinitionEntity = dmDaoHelper.getEmrClusterDefinitionEntity(emrClusterDefinitionKey);

        // Log the existing EMR cluster definition.
        LOGGER.info(String.format("EMR cluster definition being deleted:\n%s",
            xmlHelper.objectToXml(createEmrClusterDefinitionFromEntity(emrClusterDefinitionEntity), true)));

        // Delete the EMR cluster definition.
        dmDao.delete(emrClusterDefinitionEntity);

        // Create and return the EMR cluster definition object from the deleted entity.
        return createEmrClusterDefinitionFromEntity(emrClusterDefinitionEntity);
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
