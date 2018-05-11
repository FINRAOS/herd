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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import org.finra.herd.dao.BusinessObjectDefinitionDescriptionSuggestionDao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptionSuggestion;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptionSuggestionCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptionSuggestionKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptionSuggestionKeys;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptionSuggestionUpdateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.api.xml.DescriptionSuggestion;
import org.finra.herd.model.jpa.BusinessObjectDefinitionDescriptionSuggestionEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.service.BusinessObjectDefinitionDescriptionSuggestionService;
import org.finra.herd.service.helper.AlternateKeyHelper;
import org.finra.herd.service.helper.BusinessObjectDefinitionDaoHelper;
import org.finra.herd.service.helper.BusinessObjectDefinitionDescriptionSuggestionDaoHelper;

/**
 * The business object definition description suggestion service implementation.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class BusinessObjectDefinitionDescriptionSuggestionServiceImpl implements BusinessObjectDefinitionDescriptionSuggestionService
{
    @Autowired
    private AlternateKeyHelper alternateKeyHelper;

    @Autowired
    private BusinessObjectDefinitionDaoHelper businessObjectDefinitionDaoHelper;

    @Autowired
    private BusinessObjectDefinitionDescriptionSuggestionDao businessObjectDefinitionDescriptionSuggestionDao;

    @Autowired
    private BusinessObjectDefinitionDescriptionSuggestionDaoHelper businessObjectDefinitionDescriptionSuggestionDaoHelper;


    @Override
    public BusinessObjectDefinitionDescriptionSuggestion createBusinessObjectDefinitionDescriptionSuggestion(
        BusinessObjectDefinitionDescriptionSuggestionCreateRequest request)
    {
        // Validate and trim the business object definition description suggestion create request.
        validateBusinessObjectDefinitionDescriptionSuggestionCreateRequest(request);

        // Get the business object definition description suggestion key and description suggestion from the request object.
        final BusinessObjectDefinitionDescriptionSuggestionKey key = request.getBusinessObjectDefinitionDescriptionSuggestionKey();
        final DescriptionSuggestion descriptionSuggestion = request.getDescriptionSuggestion();

        // Retrieve the business object definition entity from the request.
        final BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDaoHelper
            .getBusinessObjectDefinitionEntity(new BusinessObjectDefinitionKey(key.getNamespace(), key.getBusinessObjectDefinitionName()));

        // Validate that the business object definition description suggestion does not exist.
        if (businessObjectDefinitionDescriptionSuggestionDao
            .getBusinessObjectDefinitionDescriptionSuggestionByBusinessObjectDefinitionEntityAndUserId(businessObjectDefinitionEntity, key.getUserId()) != null)
        {
            throw new AlreadyExistsException(String.format("A business object definition description suggestion already exists with the parameters " +
                    "{namespace=\"%s\", businessObjectDefinitionName=\"%s\", userId=\"%s\"}.", key.getNamespace(), key.getBusinessObjectDefinitionName(),
                key.getUserId()));
        }

        // Create a new business object definition description suggestion entity and persist the entity.
        final BusinessObjectDefinitionDescriptionSuggestionEntity businessObjectDefinitionDescriptionSuggestionEntity =
            new BusinessObjectDefinitionDescriptionSuggestionEntity();
        businessObjectDefinitionDescriptionSuggestionEntity.setBusinessObjectDefinition(businessObjectDefinitionEntity);
        businessObjectDefinitionDescriptionSuggestionEntity.setUserId(key.getUserId());
        businessObjectDefinitionDescriptionSuggestionEntity.setDescriptionSuggestion(descriptionSuggestion.getDescriptionSuggestion());
        final BusinessObjectDefinitionDescriptionSuggestionEntity createdBusinessObjectDefinitionDescriptionSuggestionEntity =
            businessObjectDefinitionDescriptionSuggestionDao.saveAndRefresh(businessObjectDefinitionDescriptionSuggestionEntity);

        return new BusinessObjectDefinitionDescriptionSuggestion(createdBusinessObjectDefinitionDescriptionSuggestionEntity.getId(), key,
            descriptionSuggestion);
    }

    @Override
    public BusinessObjectDefinitionDescriptionSuggestion deleteBusinessObjectDefinitionDescriptionSuggestion(
        BusinessObjectDefinitionDescriptionSuggestionKey key)
    {
        // Validate the business object definition description suggestion key.
        validateBusinessObjectDefinitionDescriptionSuggestionKey(key);

        // Retrieve the business object definition entity by key.
        final BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDaoHelper
            .getBusinessObjectDefinitionEntity(new BusinessObjectDefinitionKey(key.getNamespace(), key.getBusinessObjectDefinitionName()));

        // Delete the business object definition description suggestion entity.
        final BusinessObjectDefinitionDescriptionSuggestionEntity businessObjectDefinitionDescriptionSuggestionEntity =
            businessObjectDefinitionDescriptionSuggestionDaoHelper
                .getBusinessObjectDefinitionDescriptionSuggestionEntity(businessObjectDefinitionEntity, key.getUserId());
        businessObjectDefinitionDescriptionSuggestionDao.delete(businessObjectDefinitionDescriptionSuggestionEntity);

        return new BusinessObjectDefinitionDescriptionSuggestion(businessObjectDefinitionDescriptionSuggestionEntity.getId(), key,
            new DescriptionSuggestion(businessObjectDefinitionDescriptionSuggestionEntity.getDescriptionSuggestion()));
    }

    @Override
    public BusinessObjectDefinitionDescriptionSuggestion getBusinessObjectDefinitionDescriptionSuggestionByKey(
        BusinessObjectDefinitionDescriptionSuggestionKey key)
    {
        // Validate the business object definition description suggestion key.
        validateBusinessObjectDefinitionDescriptionSuggestionKey(key);

        // Retrieve the business object definition entity by key.
        final BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDaoHelper
            .getBusinessObjectDefinitionEntity(new BusinessObjectDefinitionKey(key.getNamespace(), key.getBusinessObjectDefinitionName()));

        // Get the business object definition description suggestion entity.
        final BusinessObjectDefinitionDescriptionSuggestionEntity businessObjectDefinitionDescriptionSuggestionEntity =
            businessObjectDefinitionDescriptionSuggestionDaoHelper
                .getBusinessObjectDefinitionDescriptionSuggestionEntity(businessObjectDefinitionEntity, key.getUserId());

        return new BusinessObjectDefinitionDescriptionSuggestion(businessObjectDefinitionDescriptionSuggestionEntity.getId(), key,
            new DescriptionSuggestion(businessObjectDefinitionDescriptionSuggestionEntity.getDescriptionSuggestion()));
    }

    @Override
    public BusinessObjectDefinitionDescriptionSuggestionKeys getBusinessObjectDefinitionDescriptionSuggestions(
        BusinessObjectDefinitionKey businessObjectDefinitionKey)
    {
        // Retrieve the business object definition entity by key.
        final BusinessObjectDefinitionEntity businessObjectDefinitionEntity =
            businessObjectDefinitionDaoHelper.getBusinessObjectDefinitionEntity(businessObjectDefinitionKey);

        // Return the business object definition description suggestion keys.
        return new BusinessObjectDefinitionDescriptionSuggestionKeys(businessObjectDefinitionDescriptionSuggestionDao
            .getBusinessObjectDefinitionDescriptionSuggestionsByBusinessObjectDefinitionEntity(businessObjectDefinitionEntity));
    }

    @Override
    public BusinessObjectDefinitionDescriptionSuggestion updateBusinessObjectDefinitionDescriptionSuggestion(
        BusinessObjectDefinitionDescriptionSuggestionKey key, BusinessObjectDefinitionDescriptionSuggestionUpdateRequest request)
    {
        // Validate and trim the business object definition description suggestion update request.
        validateBusinessObjectDefinitionDescriptionSuggestionUpdateRequest(request);

        // Validate the business object definition description suggestion key.
        validateBusinessObjectDefinitionDescriptionSuggestionKey(key);

        // Get the business object definition description suggestion key and description suggestion from the request object.
        final DescriptionSuggestion descriptionSuggestion = request.getDescriptionSuggestion();

        // Retrieve the business object definition entity from the request.
        final BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDaoHelper
            .getBusinessObjectDefinitionEntity(new BusinessObjectDefinitionKey(key.getNamespace(), key.getBusinessObjectDefinitionName()));

        // Update the business object definition description suggestion entity and persist the entity.
        final BusinessObjectDefinitionDescriptionSuggestionEntity businessObjectDefinitionDescriptionSuggestionEntity =
            businessObjectDefinitionDescriptionSuggestionDaoHelper
                .getBusinessObjectDefinitionDescriptionSuggestionEntity(businessObjectDefinitionEntity, key.getUserId());
        businessObjectDefinitionDescriptionSuggestionEntity.setDescriptionSuggestion(descriptionSuggestion.getDescriptionSuggestion());
        businessObjectDefinitionDescriptionSuggestionDao.saveAndRefresh(businessObjectDefinitionDescriptionSuggestionEntity);

        return new BusinessObjectDefinitionDescriptionSuggestion(businessObjectDefinitionDescriptionSuggestionEntity.getId(), key, descriptionSuggestion);
    }

    /**
     * Validates the business object definition description suggestion create request. This method also trims the request parameters.
     *
     * @param request the business object definition description suggestion create request
     */
    private void validateBusinessObjectDefinitionDescriptionSuggestionCreateRequest(BusinessObjectDefinitionDescriptionSuggestionCreateRequest request)
    {
        Assert.notNull(request, "A business object definition description suggestion create request must be specified.");

        // Validate the business object definition description suggestion key.
        validateBusinessObjectDefinitionDescriptionSuggestionKey(request.getBusinessObjectDefinitionDescriptionSuggestionKey());

        // Validate the business object definition description suggestion.
        validateBusinessObjectDefinitionDescriptionSuggestionDescriptionSuggestion(request.getDescriptionSuggestion());
    }

    /**
     * Validates the business object definition description suggestion update request.
     *
     * @param request the business object definition description suggestion update request
     */
    private void validateBusinessObjectDefinitionDescriptionSuggestionUpdateRequest(BusinessObjectDefinitionDescriptionSuggestionUpdateRequest request)
    {
        Assert.notNull(request, "A business object definition description suggestion update request must be specified.");

        // Validate the business object definition description suggestion.
        validateBusinessObjectDefinitionDescriptionSuggestionDescriptionSuggestion(request.getDescriptionSuggestion());
    }

    /**
     * Validates the business object definition description suggestion description suggestion.
     *
     * @param descriptionSuggestion the business object definition description suggestion description suggestion
     */
    private void validateBusinessObjectDefinitionDescriptionSuggestionDescriptionSuggestion(DescriptionSuggestion descriptionSuggestion)
    {
        // Validate the business object definition description suggestion.
        Assert.notNull(descriptionSuggestion, "A business object definition description suggestion must be specified.");

        // Validate the description suggestion string.
        Assert.notNull(descriptionSuggestion.getDescriptionSuggestion(), "A business object definition description suggestion must be specified.");
    }

    /**
     * Validates the business object definition description suggestion key. This method also trims the key parameters.
     *
     * @param key the business object definition description suggestion key
     */
    private void validateBusinessObjectDefinitionDescriptionSuggestionKey(BusinessObjectDefinitionDescriptionSuggestionKey key)
    {
        Assert.notNull(key, "A business object definition description suggestion key must be specified.");

        // Validate and trim the key parameters.
        key.setNamespace(alternateKeyHelper.validateStringParameter("namespace", key.getNamespace()));
        key.setBusinessObjectDefinitionName(
            alternateKeyHelper.validateStringParameter("business object definition name", key.getBusinessObjectDefinitionName()));
        key.setUserId(alternateKeyHelper.validateStringParameter("user id", key.getUserId()));
    }
}
