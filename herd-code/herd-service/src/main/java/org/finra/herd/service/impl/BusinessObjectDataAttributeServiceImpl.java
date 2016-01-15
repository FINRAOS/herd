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

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import org.finra.herd.dao.HerdDao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.api.xml.BusinessObjectDataAttribute;
import org.finra.herd.model.api.xml.BusinessObjectDataAttributeCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataAttributeKey;
import org.finra.herd.model.api.xml.BusinessObjectDataAttributeKeys;
import org.finra.herd.model.api.xml.BusinessObjectDataAttributeUpdateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.jpa.BusinessObjectDataAttributeEntity;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.service.BusinessObjectDataAttributeService;
import org.finra.herd.service.helper.HerdDaoHelper;
import org.finra.herd.service.helper.HerdHelper;

/**
 * The business object data attribute service implementation.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class BusinessObjectDataAttributeServiceImpl implements BusinessObjectDataAttributeService
{
    @Autowired
    private HerdHelper herdHelper;

    @Autowired
    private HerdDao herdDao;

    @Autowired
    private HerdDaoHelper herdDaoHelper;

    /**
     * Creates a new business object data attribute.
     *
     * @param request the information needed to create a business object data attribute
     *
     * @return the newly created business object data attribute
     */
    @Override
    public BusinessObjectDataAttribute createBusinessObjectDataAttribute(BusinessObjectDataAttributeCreateRequest request)
    {
        // Validate and trim the key.
        herdHelper.validateBusinessObjectDataAttributeKey(request.getBusinessObjectDataAttributeKey());

        // Get the business object format and ensure it exists.
        BusinessObjectFormatEntity businessObjectFormatEntity = herdDaoHelper.getBusinessObjectFormatEntity(
            new BusinessObjectFormatKey(request.getBusinessObjectDataAttributeKey().getNamespace(),
                request.getBusinessObjectDataAttributeKey().getBusinessObjectDefinitionName(),
                request.getBusinessObjectDataAttributeKey().getBusinessObjectFormatUsage(),
                request.getBusinessObjectDataAttributeKey().getBusinessObjectFormatFileType(),
                request.getBusinessObjectDataAttributeKey().getBusinessObjectFormatVersion()));

        // Validate the attribute value.
        if (herdDaoHelper.isBusinessObjectDataAttributeRequired(request.getBusinessObjectDataAttributeKey().getBusinessObjectDataAttributeName(),
            businessObjectFormatEntity))
        {
            Assert.hasText(request.getBusinessObjectDataAttributeValue(), String
                .format("A business object data attribute value must be specified since \"%s\" is a required attribute for business object format {%s}.",
                    request.getBusinessObjectDataAttributeKey().getBusinessObjectDataAttributeName(),
                    herdDaoHelper.businessObjectFormatEntityAltKeyToString(businessObjectFormatEntity)));
        }

        // Get the business object data and ensure it exists.
        BusinessObjectDataEntity businessObjectDataEntity = herdDaoHelper.getBusinessObjectDataEntity(
            new BusinessObjectDataKey(request.getBusinessObjectDataAttributeKey().getNamespace(),
                request.getBusinessObjectDataAttributeKey().getBusinessObjectDefinitionName(),
                request.getBusinessObjectDataAttributeKey().getBusinessObjectFormatUsage(),
                request.getBusinessObjectDataAttributeKey().getBusinessObjectFormatFileType(),
                request.getBusinessObjectDataAttributeKey().getBusinessObjectFormatVersion(), request.getBusinessObjectDataAttributeKey().getPartitionValue(),
                request.getBusinessObjectDataAttributeKey().getSubPartitionValues(),
                request.getBusinessObjectDataAttributeKey().getBusinessObjectDataVersion()));

        // Load all existing business object data attribute entities into a map for quick access using lowercase attribute names.
        Map<String, BusinessObjectDataAttributeEntity> businessObjectDataAttributeEntityMap =
            herdDaoHelper.getBusinessObjectDataAttributeEntityMap(businessObjectDataEntity.getAttributes());

        // Ensure a business object data attribute with the specified name doesn't already exist for the specified business object data.
        if (businessObjectDataAttributeEntityMap.containsKey(request.getBusinessObjectDataAttributeKey().getBusinessObjectDataAttributeName().toLowerCase()))
        {
            throw new AlreadyExistsException(String
                .format("Unable to create business object data attribute with name \"%s\" because it already exists for the the business object data {%s}.",
                    request.getBusinessObjectDataAttributeKey().getBusinessObjectDataAttributeName(),
                    herdDaoHelper.businessObjectDataEntityAltKeyToString(businessObjectDataEntity)));
        }

        // Create a business object data attribute entity from the request information.
        BusinessObjectDataAttributeEntity businessObjectDataAttributeEntity = createBusinessObjectDataAttributeEntity(businessObjectDataEntity, request);

        // Persist the new entity.
        businessObjectDataAttributeEntity = herdDao.saveAndRefresh(businessObjectDataAttributeEntity);

        // Create and return the business object data attribute object from the persisted entity.
        return createBusinessObjectDataAttributeFromEntity(businessObjectDataAttributeEntity);
    }

    /**
     * Gets an existing business object data attribute by key.
     *
     * @param businessObjectDataAttributeKey the business object data attribute key
     *
     * @return the business object data attribute information
     */
    @Override
    public BusinessObjectDataAttribute getBusinessObjectDataAttribute(BusinessObjectDataAttributeKey businessObjectDataAttributeKey)
    {
        // Validate and trim the key.
        herdHelper.validateBusinessObjectDataAttributeKey(businessObjectDataAttributeKey);

        // Retrieve and ensure that a business object data attribute exists with the specified key.
        BusinessObjectDataAttributeEntity businessObjectDataAttributeEntity =
            herdDaoHelper.getBusinessObjectDataAttributeEntity(businessObjectDataAttributeKey);

        // Create and return the business object data attribute object from the persisted entity.
        return createBusinessObjectDataAttributeFromEntity(businessObjectDataAttributeEntity);
    }

    /**
     * Updates an existing business object data attribute by key.
     *
     * @param businessObjectDataAttributeKey the business object data attribute key
     *
     * @return the business object data attribute information
     */
    @Override
    public BusinessObjectDataAttribute updateBusinessObjectDataAttribute(BusinessObjectDataAttributeKey businessObjectDataAttributeKey,
        BusinessObjectDataAttributeUpdateRequest request)
    {
        // Validate and trim the key.
        herdHelper.validateBusinessObjectDataAttributeKey(businessObjectDataAttributeKey);

        // Get the business object format and ensure it exists.
        BusinessObjectFormatEntity businessObjectFormatEntity = herdDaoHelper.getBusinessObjectFormatEntity(
            new BusinessObjectFormatKey(businessObjectDataAttributeKey.getNamespace(), businessObjectDataAttributeKey.getBusinessObjectDefinitionName(),
                businessObjectDataAttributeKey.getBusinessObjectFormatUsage(), businessObjectDataAttributeKey.getBusinessObjectFormatFileType(),
                businessObjectDataAttributeKey.getBusinessObjectFormatVersion()));

        // Validate the attribute value.
        if (herdDaoHelper
            .isBusinessObjectDataAttributeRequired(businessObjectDataAttributeKey.getBusinessObjectDataAttributeName(), businessObjectFormatEntity))
        {
            Assert.hasText(request.getBusinessObjectDataAttributeValue(), String
                .format("A business object data attribute value must be specified since \"%s\" is a required attribute for business object format {%s}.",
                    businessObjectDataAttributeKey.getBusinessObjectDataAttributeName(),
                    herdDaoHelper.businessObjectFormatEntityAltKeyToString(businessObjectFormatEntity)));
        }

        // Retrieve and ensure that a business object data attribute exists with the specified key.
        BusinessObjectDataAttributeEntity businessObjectDataAttributeEntity =
            herdDaoHelper.getBusinessObjectDataAttributeEntity(businessObjectDataAttributeKey);

        // Update the entity with the new values.
        businessObjectDataAttributeEntity.setValue(request.getBusinessObjectDataAttributeValue());

        // Persist the entity.
        businessObjectDataAttributeEntity = herdDao.saveAndRefresh(businessObjectDataAttributeEntity);

        // Create and return the business object data attribute object from the persisted entity.
        return createBusinessObjectDataAttributeFromEntity(businessObjectDataAttributeEntity);
    }

    /**
     * Deletes an existing business object data attribute by key.
     *
     * @param businessObjectDataAttributeKey the business object data attribute key
     *
     * @return the business object data attribute that got deleted
     */
    @Override
    public BusinessObjectDataAttribute deleteBusinessObjectDataAttribute(BusinessObjectDataAttributeKey businessObjectDataAttributeKey)
    {
        // Validate and trim the key.
        herdHelper.validateBusinessObjectDataAttributeKey(businessObjectDataAttributeKey);

        // Get the business object format and ensure it exists.
        BusinessObjectFormatEntity businessObjectFormatEntity = herdDaoHelper.getBusinessObjectFormatEntity(
            new BusinessObjectFormatKey(businessObjectDataAttributeKey.getNamespace(), businessObjectDataAttributeKey.getBusinessObjectDefinitionName(),
                businessObjectDataAttributeKey.getBusinessObjectFormatUsage(), businessObjectDataAttributeKey.getBusinessObjectFormatFileType(),
                businessObjectDataAttributeKey.getBusinessObjectFormatVersion()));

        // Make sure we are not trying to delete a required attribute.
        if (herdDaoHelper
            .isBusinessObjectDataAttributeRequired(businessObjectDataAttributeKey.getBusinessObjectDataAttributeName(), businessObjectFormatEntity))
        {
            throw new IllegalArgumentException(String.format("Cannot delete \"%s\" attribute since it is a required attribute for business object format {%s}.",
                businessObjectDataAttributeKey.getBusinessObjectDataAttributeName(),
                herdDaoHelper.businessObjectFormatEntityAltKeyToString(businessObjectFormatEntity)));
        }

        // Retrieve and ensure that a business object data attribute exists with the specified key.
        BusinessObjectDataAttributeEntity businessObjectDataAttributeEntity =
            herdDaoHelper.getBusinessObjectDataAttributeEntity(businessObjectDataAttributeKey);

        // Delete the business object data attribute.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataAttributeEntity.getBusinessObjectData();
        businessObjectDataEntity.getAttributes().remove(businessObjectDataAttributeEntity);
        herdDao.saveAndRefresh(businessObjectDataEntity);

        // Create and return the business object data attribute object from the deleted entity.
        return createBusinessObjectDataAttributeFromEntity(businessObjectDataAttributeEntity);
    }

    /**
     * Gets a list of keys for all existing business object data attributes.
     *
     * @return the business object data attribute keys
     */
    @Override
    public BusinessObjectDataAttributeKeys getBusinessObjectDataAttributes(BusinessObjectDataKey businessObjectDataKey)
    {
        // Validate and trim the business object data key.
        herdHelper.validateBusinessObjectDataKey(businessObjectDataKey, true, true);

        // Retrieve the business object data and ensure it exists.
        BusinessObjectDataEntity businessObjectDataEntity = herdDaoHelper.getBusinessObjectDataEntity(businessObjectDataKey);

        // Create and populate a list of business object data attribute keys.
        BusinessObjectDataAttributeKeys businessObjectDataAttributeKeys = new BusinessObjectDataAttributeKeys();
        for (BusinessObjectDataAttributeEntity businessObjectDataAttributeEntity : businessObjectDataEntity.getAttributes())
        {
            businessObjectDataAttributeKeys.getBusinessObjectDataAttributeKeys().add(getBusinessObjectDataAttributeKey(businessObjectDataAttributeEntity));
        }

        return businessObjectDataAttributeKeys;
    }

    /**
     * Creates a new business object data attribute entity from the business object data entity and the request information.
     *
     * @param businessObjectDataEntity the business object data entity
     * @param request the business object data attribute create request
     *
     * @return the newly created business object data attribute entity
     */
    private BusinessObjectDataAttributeEntity createBusinessObjectDataAttributeEntity(BusinessObjectDataEntity businessObjectDataEntity,
        BusinessObjectDataAttributeCreateRequest request)
    {
        // Create a new entity.
        BusinessObjectDataAttributeEntity businessObjectDataAttributeEntity = new BusinessObjectDataAttributeEntity();

        businessObjectDataAttributeEntity.setBusinessObjectData(businessObjectDataEntity);
        businessObjectDataAttributeEntity.setName(request.getBusinessObjectDataAttributeKey().getBusinessObjectDataAttributeName());
        businessObjectDataAttributeEntity.setValue(request.getBusinessObjectDataAttributeValue());

        return businessObjectDataAttributeEntity;
    }

    /**
     * Creates the business object data attribute from the persisted entity.
     *
     * @param businessObjectDataAttributeEntity the business object data attribute entity
     *
     * @return the business object data attribute
     */
    private BusinessObjectDataAttribute createBusinessObjectDataAttributeFromEntity(BusinessObjectDataAttributeEntity businessObjectDataAttributeEntity)
    {
        // Create the business object data attribute.
        BusinessObjectDataAttribute businessObjectDataAttribute = new BusinessObjectDataAttribute();

        businessObjectDataAttribute.setId(businessObjectDataAttributeEntity.getId());
        businessObjectDataAttribute.setBusinessObjectDataAttributeKey(getBusinessObjectDataAttributeKey(businessObjectDataAttributeEntity));
        businessObjectDataAttribute.setBusinessObjectDataAttributeValue(businessObjectDataAttributeEntity.getValue());

        return businessObjectDataAttribute;
    }

    /**
     * Creates and returns a business object data attribute key from a specified business object data attribute entity.
     *
     * @param businessObjectDataAttributeEntity the business object data attribute entity
     *
     * @return the newly created business object data attribute key
     */
    private BusinessObjectDataAttributeKey getBusinessObjectDataAttributeKey(BusinessObjectDataAttributeEntity businessObjectDataAttributeEntity)
    {
        return new BusinessObjectDataAttributeKey(
            businessObjectDataAttributeEntity.getBusinessObjectData().getBusinessObjectFormat().getBusinessObjectDefinition().getNamespace().getCode(),
            businessObjectDataAttributeEntity.getBusinessObjectData().getBusinessObjectFormat().getBusinessObjectDefinition().getName(),
            businessObjectDataAttributeEntity.getBusinessObjectData().getBusinessObjectFormat().getUsage(),
            businessObjectDataAttributeEntity.getBusinessObjectData().getBusinessObjectFormat().getFileType().getCode(),
            businessObjectDataAttributeEntity.getBusinessObjectData().getBusinessObjectFormat().getBusinessObjectFormatVersion(),
            businessObjectDataAttributeEntity.getBusinessObjectData().getPartitionValue(),
            herdHelper.getSubPartitionValues(businessObjectDataAttributeEntity.getBusinessObjectData()),
            businessObjectDataAttributeEntity.getBusinessObjectData().getVersion(), businessObjectDataAttributeEntity.getName());
    }
}
