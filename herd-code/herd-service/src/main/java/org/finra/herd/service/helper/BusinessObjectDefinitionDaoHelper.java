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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.dao.BusinessObjectDefinitionDao;
import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.Attribute;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.jpa.BusinessObjectDefinitionAttributeEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionChangeEventEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.model.jpa.DataProviderEntity;
import org.finra.herd.model.jpa.NamespaceEntity;


/**
 * Helper for data provider related operations which require DAO.
 */
@Component
public class BusinessObjectDefinitionDaoHelper
{
    @Autowired
    private AttributeHelper attributeHelper;

    @Autowired
    private BusinessObjectDefinitionDao businessObjectDefinitionDao;

    @Autowired
    private DataProviderDaoHelper dataProviderDaoHelper;

    @Autowired
    private NamespaceDaoHelper namespaceDaoHelper;

    @Autowired
    private AlternateKeyHelper alternateKeyHelper;

    /**
     * Retrieves a business object definition entity by it's key and ensure it exists.
     *
     * @param businessObjectDefinitionKey the business object definition name (case-insensitive)
     *
     * @return the business object definition entity
     * @throws ObjectNotFoundException if the business object definition entity doesn't exist
     */
    public BusinessObjectDefinitionEntity getBusinessObjectDefinitionEntity(BusinessObjectDefinitionKey businessObjectDefinitionKey)
        throws ObjectNotFoundException
    {
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity =
            businessObjectDefinitionDao.getBusinessObjectDefinitionByKey(businessObjectDefinitionKey);

        if (businessObjectDefinitionEntity == null)
        {
            throw new ObjectNotFoundException(String.format("Business object definition with name \"%s\" doesn't exist for namespace \"%s\".",
                businessObjectDefinitionKey.getBusinessObjectDefinitionName(), businessObjectDefinitionKey.getNamespace()));
        }

        return businessObjectDefinitionEntity;
    }

    /**
     * Create Business Object Definition Entity
     * @param request business object definition create request
     * @return Business Object Definition Entity
     */
    public BusinessObjectDefinitionEntity createBusinessObjectDefinitionEntity(BusinessObjectDefinitionCreateRequest request)
    {
        // Perform the validation.
        validateBusinessObjectDefinitionCreateRequest(request);

        // Get the namespace and ensure it exists.
        NamespaceEntity namespaceEntity = namespaceDaoHelper.getNamespaceEntity(request.getNamespace());

        // Get the data provider and ensure it exists.
        DataProviderEntity dataProviderEntity = dataProviderDaoHelper.getDataProviderEntity(request.getDataProviderName());

        // Get business object definition key.
        BusinessObjectDefinitionKey businessObjectDefinitionKey =
            new BusinessObjectDefinitionKey(request.getNamespace(), request.getBusinessObjectDefinitionName());

        // Ensure a business object definition with the specified key doesn't already exist.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity =
            businessObjectDefinitionDao.getBusinessObjectDefinitionByKey(businessObjectDefinitionKey);
        if (businessObjectDefinitionEntity != null)
        {
            throw new AlreadyExistsException(String
                .format("Unable to create business object definition with name \"%s\" because it already exists for namespace \"%s\".",
                    businessObjectDefinitionKey.getBusinessObjectDefinitionName(), businessObjectDefinitionKey.getNamespace()));
        }

        // Create a new entity.
        businessObjectDefinitionEntity = new BusinessObjectDefinitionEntity();
        businessObjectDefinitionEntity.setNamespace(namespaceEntity);
        businessObjectDefinitionEntity.setName(request.getBusinessObjectDefinitionName());
        businessObjectDefinitionEntity.setDescription(request.getDescription());
        businessObjectDefinitionEntity.setDataProvider(dataProviderEntity);
        businessObjectDefinitionEntity.setDisplayName(request.getDisplayName());

        // Create the attributes if they are specified.
        if (!CollectionUtils.isEmpty(request.getAttributes()))
        {
            List<BusinessObjectDefinitionAttributeEntity> attributeEntities = new ArrayList<>();
            businessObjectDefinitionEntity.setAttributes(attributeEntities);
            for (Attribute attribute : request.getAttributes())
            {
                BusinessObjectDefinitionAttributeEntity attributeEntity = new BusinessObjectDefinitionAttributeEntity();
                attributeEntities.add(attributeEntity);
                attributeEntity.setBusinessObjectDefinition(businessObjectDefinitionEntity);
                attributeEntity.setName(attribute.getName());
                attributeEntity.setValue(attribute.getValue());
            }
        }

        // Persist the change event entity
        saveBusinessObjectDefinitionChangeEvents(businessObjectDefinitionEntity);

        // Persist and return the new entity.
        return businessObjectDefinitionDao.saveAndRefresh(businessObjectDefinitionEntity);
    }

    /**
     * Update and persist the business object definition change events
     *
     * @param businessObjectDefinitionEntity the business object definition entity
     */
    public void saveBusinessObjectDefinitionChangeEvents(BusinessObjectDefinitionEntity businessObjectDefinitionEntity)
    {

        // Set the change events and add an entry to the change event table
        List<BusinessObjectDefinitionChangeEventEntity> businessObjectDefinitionChangeEventEntities = new ArrayList<>();
        BusinessObjectDefinitionChangeEventEntity businessObjectDefinitionChangeEventEntity = new BusinessObjectDefinitionChangeEventEntity();
        businessObjectDefinitionChangeEventEntity.setBusinessObjectDefinitionEntity(businessObjectDefinitionEntity);
        businessObjectDefinitionChangeEventEntities.add(businessObjectDefinitionChangeEventEntity);
        boolean changeEventOccurred = false;
        if (businessObjectDefinitionEntity.getDisplayName() != null)
        {
            businessObjectDefinitionChangeEventEntity.setDisplayName(businessObjectDefinitionEntity.getDisplayName());
            changeEventOccurred = true;
        }

        if (businessObjectDefinitionEntity.getDescription() != null)
        {
            businessObjectDefinitionChangeEventEntity.setDescription(businessObjectDefinitionEntity.getDescription());
            changeEventOccurred = true;
        }

        if (businessObjectDefinitionEntity.getDescriptiveBusinessObjectFormat() != null)
        {
            businessObjectDefinitionChangeEventEntity.setUsage(businessObjectDefinitionEntity.getDescriptiveBusinessObjectFormat().getUsage());
            businessObjectDefinitionChangeEventEntity.setFileType(businessObjectDefinitionEntity.getDescriptiveBusinessObjectFormat().getFileType().getCode());
            changeEventOccurred = true;
        }

        if (changeEventOccurred)
        {
            if (businessObjectDefinitionEntity.getChangeEvents() != null)
            {
                businessObjectDefinitionEntity.getChangeEvents().add(businessObjectDefinitionChangeEventEntity);
            }
            else
            {
                businessObjectDefinitionEntity.setChangeEvents(businessObjectDefinitionChangeEventEntities);
            }
        }
    }

    /**
     * Validates the business object definition create request. This method also trims request parameters.
     *
     * @param request the request
     */
    private void validateBusinessObjectDefinitionCreateRequest(BusinessObjectDefinitionCreateRequest request)
    {
        request.setNamespace(alternateKeyHelper.validateStringParameter("namespace", request.getNamespace()));
        request.setBusinessObjectDefinitionName(
            alternateKeyHelper.validateStringParameter("business object definition name", request.getBusinessObjectDefinitionName()));
        request.setDataProviderName(alternateKeyHelper.validateStringParameter("data provider name", request.getDataProviderName()));

        if (request.getDisplayName() != null)
        {
            request.setDisplayName(request.getDisplayName().trim());
        }

        // Validate attributes.
        attributeHelper.validateAttributes(request.getAttributes());
    }
}
