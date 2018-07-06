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

import static org.finra.herd.model.dto.SearchIndexUpdateDto.SEARCH_INDEX_UPDATE_TYPE_UPDATE;

import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import org.finra.herd.core.HerdDateUtils;
import org.finra.herd.dao.BusinessObjectDefinitionDao;
import org.finra.herd.dao.BusinessObjectDefinitionDescriptionSuggestionDao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.annotation.NamespacePermission;
import org.finra.herd.model.annotation.PublishNotificationMessages;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptionSuggestion;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptionSuggestionAcceptanceRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptionSuggestionCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptionSuggestionKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptionSuggestionKeys;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptionSuggestionSearchFilter;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptionSuggestionSearchKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptionSuggestionSearchRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptionSuggestionSearchResponse;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptionSuggestionUpdateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.api.xml.NamespacePermissionEnum;
import org.finra.herd.model.jpa.BusinessObjectDefinitionDescriptionSuggestionEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionDescriptionSuggestionStatusEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.service.BusinessObjectDefinitionDescriptionSuggestionService;
import org.finra.herd.service.MessageNotificationEventService;
import org.finra.herd.service.SearchableService;
import org.finra.herd.service.helper.AlternateKeyHelper;
import org.finra.herd.service.helper.BusinessObjectDefinitionDaoHelper;
import org.finra.herd.service.helper.BusinessObjectDefinitionDescriptionSuggestionDaoHelper;
import org.finra.herd.service.helper.BusinessObjectDefinitionDescriptionSuggestionStatusDaoHelper;
import org.finra.herd.service.helper.BusinessObjectDefinitionHelper;
import org.finra.herd.service.helper.SearchIndexUpdateHelper;

/**
 * The business object definition description suggestion service implementation.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class BusinessObjectDefinitionDescriptionSuggestionServiceImpl implements BusinessObjectDefinitionDescriptionSuggestionService, SearchableService
{
    // Constant to hold the created by user ID field option for the search response.
    public final static String CREATED_BY_USER_ID_FIELD = "createdByUserId".toLowerCase();

    // Constant to hold the created on field option for the search response.
    public final static String CREATED_ON_FIELD = "createdOn".toLowerCase();

    // Constant to hold the description suggestion field option for the search response.
    public final static String DESCRIPTION_SUGGESTION_FIELD = "descriptionSuggestion".toLowerCase();

    // Constant to hold the status field option for the search response.
    public final static String STATUS_FIELD = "status".toLowerCase();

    @Autowired
    private AlternateKeyHelper alternateKeyHelper;

    @Autowired
    private BusinessObjectDefinitionDao businessObjectDefinitionDao;

    @Autowired
    private BusinessObjectDefinitionDaoHelper businessObjectDefinitionDaoHelper;

    @Autowired
    private BusinessObjectDefinitionDescriptionSuggestionDao businessObjectDefinitionDescriptionSuggestionDao;

    @Autowired
    private BusinessObjectDefinitionDescriptionSuggestionDaoHelper businessObjectDefinitionDescriptionSuggestionDaoHelper;

    @Autowired
    private BusinessObjectDefinitionDescriptionSuggestionStatusDaoHelper businessObjectDefinitionDescriptionSuggestionStatusDaoHelper;

    @Autowired
    private BusinessObjectDefinitionHelper businessObjectDefinitionHelper;

    @Autowired
    private MessageNotificationEventService messageNotificationEventService;

    @Autowired
    private SearchIndexUpdateHelper searchIndexUpdateHelper;

    @PublishNotificationMessages
    @Override
    public BusinessObjectDefinitionDescriptionSuggestion createBusinessObjectDefinitionDescriptionSuggestion(
        BusinessObjectDefinitionDescriptionSuggestionCreateRequest request)
    {
        // Validate and trim the business object definition description suggestion create request.
        validateBusinessObjectDefinitionDescriptionSuggestionCreateRequest(request);

        // Get the business object definition description suggestion key and description suggestion from the request object.
        final BusinessObjectDefinitionDescriptionSuggestionKey key = request.getBusinessObjectDefinitionDescriptionSuggestionKey();

        // Retrieve the business object definition entity from the request.
        final BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDaoHelper
            .getBusinessObjectDefinitionEntity(new BusinessObjectDefinitionKey(key.getNamespace(), key.getBusinessObjectDefinitionName()));

        // Validate that the business object definition description suggestion does not exist.
        if (businessObjectDefinitionDescriptionSuggestionDao
            .getBusinessObjectDefinitionDescriptionSuggestionByBusinessObjectDefinitionAndUserId(businessObjectDefinitionEntity, key.getUserId()) != null)
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
        businessObjectDefinitionDescriptionSuggestionEntity.setDescriptionSuggestion(request.getDescriptionSuggestion());
        businessObjectDefinitionDescriptionSuggestionEntity.setStatus(businessObjectDefinitionDescriptionSuggestionStatusDaoHelper
            .getBusinessObjectDefinitionDescriptionSuggestionStatusEntity(
                BusinessObjectDefinitionDescriptionSuggestionStatusEntity.BusinessObjectDefinitionDescriptionSuggestionStatuses.PENDING.name()));
        final BusinessObjectDefinitionDescriptionSuggestionEntity createdBusinessObjectDefinitionDescriptionSuggestionEntity =
            businessObjectDefinitionDescriptionSuggestionDao.saveAndRefresh(businessObjectDefinitionDescriptionSuggestionEntity);

        // Create a business object definition description suggestion from the persisted entity.
        BusinessObjectDefinitionDescriptionSuggestion businessObjectDefinitionDescriptionSuggestion =
            createBusinessObjectDefinitionDescriptionSuggestionFromEntity(createdBusinessObjectDefinitionDescriptionSuggestionEntity);

        // Process a business object definition description suggestion change notification event.
        messageNotificationEventService
            .processBusinessObjectDefinitionDescriptionSuggestionChangeNotificationEvent(businessObjectDefinitionDescriptionSuggestion,
                createdBusinessObjectDefinitionDescriptionSuggestionEntity.getUpdatedBy(),
                HerdDateUtils.getXMLGregorianCalendarValue(createdBusinessObjectDefinitionDescriptionSuggestionEntity.getUpdatedOn()),
                createdBusinessObjectDefinitionDescriptionSuggestionEntity.getBusinessObjectDefinition().getNamespace());

        // Return the business object definition description suggestion created from the persisted entity.
        return businessObjectDefinitionDescriptionSuggestion;
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

        // Create and return a business object definition description suggestion.
        return createBusinessObjectDefinitionDescriptionSuggestionFromEntity(businessObjectDefinitionDescriptionSuggestionEntity);
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

        // Create and return a business object definition description suggestion.
        return createBusinessObjectDefinitionDescriptionSuggestionFromEntity(businessObjectDefinitionDescriptionSuggestionEntity);
    }

    @Override
    public BusinessObjectDefinitionDescriptionSuggestionKeys getBusinessObjectDefinitionDescriptionSuggestions(
        BusinessObjectDefinitionKey businessObjectDefinitionKey)
    {
        // Perform validation and trim.
        businessObjectDefinitionHelper.validateBusinessObjectDefinitionKey(businessObjectDefinitionKey);

        // Retrieve the business object definition entity by key.
        final BusinessObjectDefinitionEntity businessObjectDefinitionEntity =
            businessObjectDefinitionDaoHelper.getBusinessObjectDefinitionEntity(businessObjectDefinitionKey);

        // Return the business object definition description suggestion keys.
        return new BusinessObjectDefinitionDescriptionSuggestionKeys(businessObjectDefinitionDescriptionSuggestionDao
            .getBusinessObjectDefinitionDescriptionSuggestionsByBusinessObjectDefinition(businessObjectDefinitionEntity));
    }

    /**
     * Returns the valid search fields.
     *
     * @return valid search fields
     */
    @Override
    public Set<String> getValidSearchResponseFields()
    {
        return ImmutableSet.of(CREATED_BY_USER_ID_FIELD, CREATED_ON_FIELD, DESCRIPTION_SUGGESTION_FIELD, STATUS_FIELD);
    }

    @Override
    public BusinessObjectDefinitionDescriptionSuggestionSearchResponse searchBusinessObjectDefinitionDescriptionSuggestions(
        BusinessObjectDefinitionDescriptionSuggestionSearchRequest request, Set<String> fields)
    {
        // Validate search request
        validateBusinessObjectDefinitionDescriptionSuggestionSearchRequest(request);

        // Validate the fields
        validateSearchResponseFields(fields);

        // Only a single search filter and a single search key is allowed at this time.
        // Use the first search filter and first search key in the filter and keys list.
        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(
            request.getBusinessObjectDefinitionDescriptionSuggestionSearchFilters().get(0).getBusinessObjectDefinitionDescriptionSuggestionSearchKeys().get(0)
                .getNamespace(),
            request.getBusinessObjectDefinitionDescriptionSuggestionSearchFilters().get(0).getBusinessObjectDefinitionDescriptionSuggestionSearchKeys().get(0)
                .getBusinessObjectDefinitionName());
        String status =
            request.getBusinessObjectDefinitionDescriptionSuggestionSearchFilters().get(0).getBusinessObjectDefinitionDescriptionSuggestionSearchKeys().get(0)
                .getStatus();

        // The list of business object definition description suggestions
        List<BusinessObjectDefinitionDescriptionSuggestionEntity> businessObjectDefinitionDescriptionSuggestionEntities =
            businessObjectDefinitionDescriptionSuggestionDaoHelper
                .getBusinessObjectDefinitionDescriptionSuggestionsByBusinessObjectDefinitionAndStatus(businessObjectDefinitionKey, status);

        // Populate the business object definition description suggestions list.
        List<BusinessObjectDefinitionDescriptionSuggestion> businessObjectDefinitionDescriptionSuggestions = Lists.newArrayList();
        for (BusinessObjectDefinitionDescriptionSuggestionEntity entity : businessObjectDefinitionDescriptionSuggestionEntities)
        {
            businessObjectDefinitionDescriptionSuggestions.add(createBusinessObjectDefinitionDescriptionSuggestionFromEntity(entity, fields));
        }

        return new BusinessObjectDefinitionDescriptionSuggestionSearchResponse(businessObjectDefinitionDescriptionSuggestions);
    }

    @PublishNotificationMessages
    @Override
    public BusinessObjectDefinitionDescriptionSuggestion updateBusinessObjectDefinitionDescriptionSuggestion(
        BusinessObjectDefinitionDescriptionSuggestionKey key, BusinessObjectDefinitionDescriptionSuggestionUpdateRequest request)
    {
        // Validate and trim the business object definition description suggestion update request.
        validateBusinessObjectDefinitionDescriptionSuggestionUpdateRequest(request);

        // Validate the business object definition description suggestion key.
        validateBusinessObjectDefinitionDescriptionSuggestionKey(key);

        // Retrieve the business object definition entity from the request.
        final BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDaoHelper
            .getBusinessObjectDefinitionEntity(new BusinessObjectDefinitionKey(key.getNamespace(), key.getBusinessObjectDefinitionName()));

        // Update the business object definition description suggestion entity and persist the entity.
        final BusinessObjectDefinitionDescriptionSuggestionEntity businessObjectDefinitionDescriptionSuggestionEntity =
            businessObjectDefinitionDescriptionSuggestionDaoHelper
                .getBusinessObjectDefinitionDescriptionSuggestionEntity(businessObjectDefinitionEntity, key.getUserId());
        businessObjectDefinitionDescriptionSuggestionEntity.setDescriptionSuggestion(request.getDescriptionSuggestion());
        businessObjectDefinitionDescriptionSuggestionDao.saveAndRefresh(businessObjectDefinitionDescriptionSuggestionEntity);

        // Create a business object definition description suggestion from the updated entity.
        BusinessObjectDefinitionDescriptionSuggestion businessObjectDefinitionDescriptionSuggestion =
            createBusinessObjectDefinitionDescriptionSuggestionFromEntity(businessObjectDefinitionDescriptionSuggestionEntity);

        // Process a business object definition description suggestion change notification event.
        messageNotificationEventService
            .processBusinessObjectDefinitionDescriptionSuggestionChangeNotificationEvent(businessObjectDefinitionDescriptionSuggestion,
                businessObjectDefinitionDescriptionSuggestionEntity.getUpdatedBy(),
                HerdDateUtils.getXMLGregorianCalendarValue(businessObjectDefinitionDescriptionSuggestionEntity.getUpdatedOn()),
                businessObjectDefinitionDescriptionSuggestionEntity.getBusinessObjectDefinition().getNamespace());

        // Return the business object definition description suggestion created from the updated entity.
        return businessObjectDefinitionDescriptionSuggestion;
    }

    @NamespacePermission(fields = "#request.businessObjectDefinitionDescriptionSuggestionKey.namespace", permissions = {
        NamespacePermissionEnum.WRITE_DESCRIPTIVE_CONTENT, NamespacePermissionEnum.WRITE})
    @Override
    public BusinessObjectDefinitionDescriptionSuggestion acceptBusinessObjectDefinitionDescriptionSuggestion(
        BusinessObjectDefinitionDescriptionSuggestionAcceptanceRequest request)
    {
        // Validate and trim the business object definition description suggestion acceptance request.
        validateBusinessObjectDefinitionDescriptionSuggestionAcceptanceRequest(request);

        // Get the business object definition description suggestion key from the request.
        BusinessObjectDefinitionDescriptionSuggestionKey businessObjectDefinitionDescriptionSuggestionKey =
            request.getBusinessObjectDefinitionDescriptionSuggestionKey();

        // Get the business object definition key from the request.
        BusinessObjectDefinitionKey businessObjectDefinitionKey =
            new BusinessObjectDefinitionKey(businessObjectDefinitionDescriptionSuggestionKey.getNamespace(),
                businessObjectDefinitionDescriptionSuggestionKey.getBusinessObjectDefinitionName());

        // Get business object definition entity and make sure it exists.
        final BusinessObjectDefinitionEntity businessObjectDefinitionEntity =
            businessObjectDefinitionDaoHelper.getBusinessObjectDefinitionEntity(businessObjectDefinitionKey);

        // Get business object definition description suggestion entity and make sure it exists.
        final BusinessObjectDefinitionDescriptionSuggestionEntity businessObjectDefinitionDescriptionSuggestionEntity =
            businessObjectDefinitionDescriptionSuggestionDaoHelper.getBusinessObjectDefinitionDescriptionSuggestionEntity(businessObjectDefinitionEntity,
                businessObjectDefinitionDescriptionSuggestionKey.getUserId());

        // Check if retrieved entity has PENDING status.
        Assert.isTrue(StringUtils
            .equals(BusinessObjectDefinitionDescriptionSuggestionStatusEntity.BusinessObjectDefinitionDescriptionSuggestionStatuses.PENDING.name(),
                businessObjectDefinitionDescriptionSuggestionEntity.getStatus().getCode()), String
            .format("A business object definition description suggestion status is expected to be \"%s\" but was \"%s\".",
                BusinessObjectDefinitionDescriptionSuggestionStatusEntity.BusinessObjectDefinitionDescriptionSuggestionStatuses.PENDING.name(),
                businessObjectDefinitionDescriptionSuggestionEntity.getStatus().getCode()));

        // Get business object definition description suggestion status entity for ACCEPTED status and make sure it exists.
        // Update the business object definition description suggestion entity with the new status.
        businessObjectDefinitionDescriptionSuggestionEntity.setStatus(businessObjectDefinitionDescriptionSuggestionStatusDaoHelper
            .getBusinessObjectDefinitionDescriptionSuggestionStatusEntity(
                BusinessObjectDefinitionDescriptionSuggestionStatusEntity.BusinessObjectDefinitionDescriptionSuggestionStatuses.ACCEPTED.name()));
        businessObjectDefinitionDescriptionSuggestionDao.saveAndRefresh(businessObjectDefinitionDescriptionSuggestionEntity);

        // Update description of the business object definition and save this change event in the business object definition history table.
        businessObjectDefinitionEntity.setDescription(businessObjectDefinitionDescriptionSuggestionEntity.getDescriptionSuggestion());
        businessObjectDefinitionDaoHelper.saveBusinessObjectDefinitionChangeEvents(businessObjectDefinitionEntity);
        businessObjectDefinitionDao.saveAndRefresh(businessObjectDefinitionEntity);

        // Notify the search index that a business object definition must be updated.
        searchIndexUpdateHelper.modifyBusinessObjectDefinitionInSearchIndex(businessObjectDefinitionEntity, SEARCH_INDEX_UPDATE_TYPE_UPDATE);

        // Create and return a business object definition description suggestion.
        return createBusinessObjectDefinitionDescriptionSuggestionFromEntity(businessObjectDefinitionDescriptionSuggestionEntity);
    }

    /**
     * Creates a business object definition description suggestion from the persisted entity.
     *
     * @param businessObjectDefinitionDescriptionSuggestionEntity the business object definition description suggestion entity
     *
     * @return the business object definition description suggestion
     */
    private BusinessObjectDefinitionDescriptionSuggestion createBusinessObjectDefinitionDescriptionSuggestionFromEntity(
        BusinessObjectDefinitionDescriptionSuggestionEntity businessObjectDefinitionDescriptionSuggestionEntity)
    {
        return new BusinessObjectDefinitionDescriptionSuggestion(businessObjectDefinitionDescriptionSuggestionEntity.getId(),
            new BusinessObjectDefinitionDescriptionSuggestionKey(
                businessObjectDefinitionDescriptionSuggestionEntity.getBusinessObjectDefinition().getNamespace().getCode(),
                businessObjectDefinitionDescriptionSuggestionEntity.getBusinessObjectDefinition().getName(),
                businessObjectDefinitionDescriptionSuggestionEntity.getUserId()),
            businessObjectDefinitionDescriptionSuggestionEntity.getDescriptionSuggestion(),
            businessObjectDefinitionDescriptionSuggestionEntity.getStatus().getCode(), businessObjectDefinitionDescriptionSuggestionEntity.getCreatedBy(),
            HerdDateUtils.getXMLGregorianCalendarValue(businessObjectDefinitionDescriptionSuggestionEntity.getCreatedOn()));
    }

    /**
     * Creates a business object definition description suggestion from the persisted entity.
     *
     * @param businessObjectDefinitionDescriptionSuggestionEntity the business object definition description suggestion entity
     * @param fields set of field parameters to include on the business object definition description suggestion
     *
     * @return the business object definition description suggestion
     */
    private BusinessObjectDefinitionDescriptionSuggestion createBusinessObjectDefinitionDescriptionSuggestionFromEntity(
        BusinessObjectDefinitionDescriptionSuggestionEntity businessObjectDefinitionDescriptionSuggestionEntity, Set<String> fields)
    {
        BusinessObjectDefinitionDescriptionSuggestion businessObjectDefinitionDescriptionSuggestion = new BusinessObjectDefinitionDescriptionSuggestion();

        businessObjectDefinitionDescriptionSuggestion.setId(businessObjectDefinitionDescriptionSuggestionEntity.getId());
        businessObjectDefinitionDescriptionSuggestion.setBusinessObjectDefinitionDescriptionSuggestionKey(
            getBusinessObjectDefinitionDescriptionSuggestionKey(businessObjectDefinitionDescriptionSuggestionEntity));

        if (fields.contains(CREATED_BY_USER_ID_FIELD))
        {
            businessObjectDefinitionDescriptionSuggestion.setCreatedByUserId(businessObjectDefinitionDescriptionSuggestionEntity.getCreatedBy());
        }

        if (fields.contains(CREATED_ON_FIELD))
        {
            businessObjectDefinitionDescriptionSuggestion
                .setCreatedOn(HerdDateUtils.getXMLGregorianCalendarValue(businessObjectDefinitionDescriptionSuggestionEntity.getCreatedOn()));
        }

        if (fields.contains(DESCRIPTION_SUGGESTION_FIELD))
        {
            businessObjectDefinitionDescriptionSuggestion
                .setDescriptionSuggestion(businessObjectDefinitionDescriptionSuggestionEntity.getDescriptionSuggestion());
        }

        if (fields.contains(STATUS_FIELD))
        {
            businessObjectDefinitionDescriptionSuggestion.setStatus(businessObjectDefinitionDescriptionSuggestionEntity.getStatus().getCode());
        }

        return businessObjectDefinitionDescriptionSuggestion;
    }

    /**
     * Creates a business object definition description suggestion key from the entity.
     *
     * @param businessObjectDefinitionDescriptionSuggestionEntity the business object definition entity
     *
     * @return the business object definition description suggestion key
     */
    private BusinessObjectDefinitionDescriptionSuggestionKey getBusinessObjectDefinitionDescriptionSuggestionKey(
        BusinessObjectDefinitionDescriptionSuggestionEntity businessObjectDefinitionDescriptionSuggestionEntity)
    {
        return new BusinessObjectDefinitionDescriptionSuggestionKey(
            businessObjectDefinitionDescriptionSuggestionEntity.getBusinessObjectDefinition().getNamespace().getCode(),
            businessObjectDefinitionDescriptionSuggestionEntity.getBusinessObjectDefinition().getName(),
            businessObjectDefinitionDescriptionSuggestionEntity.getUserId());
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
        Assert.notNull(request.getDescriptionSuggestion(), "A business object definition description suggestion must be specified.");
    }

    /**
     * Validates the business object definition description suggestion search request.
     *
     * @param request the business object definition description suggestion search request
     *
     * @throws IllegalArgumentException if any validation errors were found
     */
    private void validateBusinessObjectDefinitionDescriptionSuggestionSearchRequest(BusinessObjectDefinitionDescriptionSuggestionSearchRequest request)
        throws IllegalArgumentException
    {
        // Validate the request
        Assert.notNull(request, "A business object definition description suggestion search request must be specified.");

        // Validate the search filters
        List<BusinessObjectDefinitionDescriptionSuggestionSearchFilter> businessObjectDefinitionDescriptionSuggestionSearchFilters =
            request.getBusinessObjectDefinitionDescriptionSuggestionSearchFilters();
        Assert.isTrue(CollectionUtils.size(businessObjectDefinitionDescriptionSuggestionSearchFilters) == 1 &&
                businessObjectDefinitionDescriptionSuggestionSearchFilters.get(0) != null,
            "Exactly one business object definition description suggestion search filter must be specified.");

        // Validate the search keys
        List<BusinessObjectDefinitionDescriptionSuggestionSearchKey> businessObjectDefinitionDescriptionSuggestionSearchKeys =
            businessObjectDefinitionDescriptionSuggestionSearchFilters.get(0).getBusinessObjectDefinitionDescriptionSuggestionSearchKeys();
        Assert.isTrue(CollectionUtils.size(businessObjectDefinitionDescriptionSuggestionSearchKeys) == 1 &&
                businessObjectDefinitionDescriptionSuggestionSearchKeys.get(0) != null,
            "Exactly one business object definition description suggestion search key must be specified.");

        // Get the search key
        BusinessObjectDefinitionDescriptionSuggestionSearchKey businessObjectDefinitionDescriptionSuggestionSearchKey =
            businessObjectDefinitionDescriptionSuggestionSearchKeys.get(0);

        // Validate the namespace and the business object definition
        businessObjectDefinitionDescriptionSuggestionSearchKey
            .setNamespace(alternateKeyHelper.validateStringParameter("namespace", businessObjectDefinitionDescriptionSuggestionSearchKey.getNamespace()));
        businessObjectDefinitionDescriptionSuggestionSearchKey.setBusinessObjectDefinitionName(alternateKeyHelper
            .validateStringParameter("business object definition name",
                businessObjectDefinitionDescriptionSuggestionSearchKey.getBusinessObjectDefinitionName()));

        // If there is a status string, then trim it.
        if (StringUtils.isNotEmpty(businessObjectDefinitionDescriptionSuggestionSearchKey.getStatus()))
        {
            businessObjectDefinitionDescriptionSuggestionSearchKey.setStatus(businessObjectDefinitionDescriptionSuggestionSearchKey.getStatus().trim());
        }
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
        Assert.notNull(request.getDescriptionSuggestion(), "A business object definition description suggestion must be specified.");
    }

    /**
     * Validates the business object definition description suggestion acceptance request.
     *
     * @param request the business object definition description suggestion acceptance request
     */
    private void validateBusinessObjectDefinitionDescriptionSuggestionAcceptanceRequest(BusinessObjectDefinitionDescriptionSuggestionAcceptanceRequest request)
    {
        Assert.notNull(request, "A business object definition description suggestion acceptance request must be specified.");

        // Validate the business object definition description suggestion key.
        validateBusinessObjectDefinitionDescriptionSuggestionKey(request.getBusinessObjectDefinitionDescriptionSuggestionKey());
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
