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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import org.finra.herd.dao.BusinessObjectDefinitionDao;
import org.finra.herd.dao.BusinessObjectDefinitionSubjectMatterExpertDao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionSubjectMatterExpert;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionSubjectMatterExpertCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionSubjectMatterExpertKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionSubjectMatterExpertKeys;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionSubjectMatterExpertEntity;
import org.finra.herd.service.BusinessObjectDefinitionSubjectMatterExpertService;
import org.finra.herd.service.helper.AlternateKeyHelper;
import org.finra.herd.service.helper.BusinessObjectDefinitionDaoHelper;
import org.finra.herd.service.helper.BusinessObjectDefinitionHelper;
import org.finra.herd.service.helper.BusinessObjectDefinitionSubjectMatterExpertDaoHelper;
import org.finra.herd.service.helper.SearchIndexUpdateHelper;

/**
 * The business object definition subject matter expert service implementation.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class BusinessObjectDefinitionSubjectMatterExpertServiceImpl implements BusinessObjectDefinitionSubjectMatterExpertService
{
    @Autowired
    private AlternateKeyHelper alternateKeyHelper;

    @Autowired
    private BusinessObjectDefinitionDao businessObjectDefinitionDao;

    @Autowired
    private BusinessObjectDefinitionDaoHelper businessObjectDefinitionDaoHelper;

    @Autowired
    private BusinessObjectDefinitionHelper businessObjectDefinitionHelper;

    @Autowired
    private BusinessObjectDefinitionSubjectMatterExpertDao businessObjectDefinitionSubjectMatterExpertDao;

    @Autowired
    private BusinessObjectDefinitionSubjectMatterExpertDaoHelper businessObjectDefinitionSubjectMatterExpertDaoHelper;

    @Autowired
    private SearchIndexUpdateHelper searchIndexUpdateHelper;

    @Override
    public BusinessObjectDefinitionSubjectMatterExpert createBusinessObjectDefinitionSubjectMatterExpert(
        BusinessObjectDefinitionSubjectMatterExpertCreateRequest request)
    {
        // Validate and trim the business object definition subject matter expert create request.
        validateBusinessObjectDefinitionSubjectMatterExpertCreateRequest(request);

        // Get the business object definition key.
        BusinessObjectDefinitionKey businessObjectDefinitionKey =
            new BusinessObjectDefinitionKey(request.getBusinessObjectDefinitionSubjectMatterExpertKey().getNamespace(),
                request.getBusinessObjectDefinitionSubjectMatterExpertKey().getBusinessObjectDefinitionName());

        // Get the business object definition and ensure it exists.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity =
            businessObjectDefinitionDaoHelper.getBusinessObjectDefinitionEntity(businessObjectDefinitionKey);

        // Ensure a business object definition subject matter expert with the specified name doesn't already exist for the business object definition.
        if (businessObjectDefinitionSubjectMatterExpertDao.getBusinessObjectDefinitionSubjectMatterExpert(businessObjectDefinitionEntity,
            request.getBusinessObjectDefinitionSubjectMatterExpertKey().getUserId()) != null)
        {
            throw new AlreadyExistsException(String.format("Unable to create business object definition subject matter expert with user id \"%s\" " +
                "because it already exists for the business object definition {%s}.", request.getBusinessObjectDefinitionSubjectMatterExpertKey().getUserId(),
                businessObjectDefinitionHelper.businessObjectDefinitionKeyToString(businessObjectDefinitionKey)));
        }

        // Create a business object definition subject matter expert entity from the request information.
        BusinessObjectDefinitionSubjectMatterExpertEntity businessObjectDefinitionSubjectMatterExpertEntity =
            createBusinessObjectDefinitionSubjectMatterExpertEntity(businessObjectDefinitionEntity, request);

        // Persist the new entity.
        businessObjectDefinitionSubjectMatterExpertEntity =
            businessObjectDefinitionSubjectMatterExpertDao.saveAndRefresh(businessObjectDefinitionSubjectMatterExpertEntity);

        // Notify the search index that a business object definition must be updated.
        searchIndexUpdateHelper.modifyBusinessObjectDefinitionInSearchIndex(businessObjectDefinitionEntity, SEARCH_INDEX_UPDATE_TYPE_UPDATE);

        // Create and return the business object definition subject matter expert object from the persisted entity.
        return createBusinessObjectDefinitionSubjectMatterExpertFromEntity(businessObjectDefinitionSubjectMatterExpertEntity);
    }

    @Override
    public BusinessObjectDefinitionSubjectMatterExpert deleteBusinessObjectDefinitionSubjectMatterExpert(BusinessObjectDefinitionSubjectMatterExpertKey key)
    {
        // Validate and trim the business object definition subject matter expert key.
        validateBusinessObjectDefinitionSubjectMatterExpertKey(key);

        // Retrieve and ensure that a business object definition subject matter expert exists.
        BusinessObjectDefinitionSubjectMatterExpertEntity businessObjectDefinitionSubjectMatterExpertEntity =
            businessObjectDefinitionSubjectMatterExpertDaoHelper.getBusinessObjectDefinitionSubjectMatterExpertEntity(key);

        // Delete the business object definition subject matter expert.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionSubjectMatterExpertEntity.getBusinessObjectDefinition();
        businessObjectDefinitionEntity.getSubjectMatterExperts().remove(businessObjectDefinitionSubjectMatterExpertEntity);
        businessObjectDefinitionDao.saveAndRefresh(businessObjectDefinitionEntity);

        // Notify the search index that a business object definition must be updated.
        searchIndexUpdateHelper.modifyBusinessObjectDefinitionInSearchIndex(businessObjectDefinitionEntity, SEARCH_INDEX_UPDATE_TYPE_UPDATE);

        // Create and return the business object definition subject matter expert object from the deleted entity.
        return createBusinessObjectDefinitionSubjectMatterExpertFromEntity(businessObjectDefinitionSubjectMatterExpertEntity);
    }

    @Override
    public BusinessObjectDefinitionSubjectMatterExpertKeys getBusinessObjectDefinitionSubjectMatterExpertsByBusinessObjectDefinition(
        BusinessObjectDefinitionKey businessObjectDefinitionKey)
    {
        // Validate and trim the business object definition key.
        businessObjectDefinitionHelper.validateBusinessObjectDefinitionKey(businessObjectDefinitionKey);

        // Retrieve the business object definition and ensure it exists.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity =
            businessObjectDefinitionDaoHelper.getBusinessObjectDefinitionEntity(businessObjectDefinitionKey);

        // Create and populate a list of business object definition subject matter expert keys.
        BusinessObjectDefinitionSubjectMatterExpertKeys businessObjectDefinitionSubjectMatterExpertKeys = new BusinessObjectDefinitionSubjectMatterExpertKeys();
        for (BusinessObjectDefinitionSubjectMatterExpertEntity businessObjectDefinitionSubjectMatterExpertEntity : businessObjectDefinitionEntity
            .getSubjectMatterExperts())
        {
            businessObjectDefinitionSubjectMatterExpertKeys.getBusinessObjectDefinitionSubjectMatterExpertKeys()
                .add(getBusinessObjectDefinitionSubjectMatterExpertKey(businessObjectDefinitionSubjectMatterExpertEntity));
        }

        return businessObjectDefinitionSubjectMatterExpertKeys;
    }

    /**
     * Creates and persists a new business object definition subject matter expert entity from the business object definition entity and the request
     * information.
     *
     * @param businessObjectDefinitionEntity the business object definition entity
     * @param request the business object definition subject matter expert create request
     *
     * @return the newly created business object definition subject matter expert entity
     */
    private BusinessObjectDefinitionSubjectMatterExpertEntity createBusinessObjectDefinitionSubjectMatterExpertEntity(
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity, BusinessObjectDefinitionSubjectMatterExpertCreateRequest request)
    {
        BusinessObjectDefinitionSubjectMatterExpertEntity businessObjectDefinitionSubjectMatterExpertEntity =
            new BusinessObjectDefinitionSubjectMatterExpertEntity();

        businessObjectDefinitionSubjectMatterExpertEntity.setBusinessObjectDefinition(businessObjectDefinitionEntity);
        businessObjectDefinitionSubjectMatterExpertEntity.setUserId(request.getBusinessObjectDefinitionSubjectMatterExpertKey().getUserId());

        return businessObjectDefinitionSubjectMatterExpertDao.saveAndRefresh(businessObjectDefinitionSubjectMatterExpertEntity);
    }

    /**
     * Creates a business object definition subject matter expert from the persisted entity.
     *
     * @param businessObjectDefinitionSubjectMatterExpertEntity the business object definition subject matter expert entity
     *
     * @return the business object definition subject matter expert
     */
    private BusinessObjectDefinitionSubjectMatterExpert createBusinessObjectDefinitionSubjectMatterExpertFromEntity(
        BusinessObjectDefinitionSubjectMatterExpertEntity businessObjectDefinitionSubjectMatterExpertEntity)
    {
        return new BusinessObjectDefinitionSubjectMatterExpert(businessObjectDefinitionSubjectMatterExpertEntity.getId(),
            getBusinessObjectDefinitionSubjectMatterExpertKey(businessObjectDefinitionSubjectMatterExpertEntity));
    }

    /**
     * Creates a business object definition subject matter expert key from the entity.
     *
     * @param businessObjectDefinitionSubjectMatterExpertEntity the business object definition subject matter expert entity
     *
     * @return the business object definition subject matter expert key
     */
    private BusinessObjectDefinitionSubjectMatterExpertKey getBusinessObjectDefinitionSubjectMatterExpertKey(
        BusinessObjectDefinitionSubjectMatterExpertEntity businessObjectDefinitionSubjectMatterExpertEntity)
    {
        return new BusinessObjectDefinitionSubjectMatterExpertKey(
            businessObjectDefinitionSubjectMatterExpertEntity.getBusinessObjectDefinition().getNamespace().getCode(),
            businessObjectDefinitionSubjectMatterExpertEntity.getBusinessObjectDefinition().getName(),
            businessObjectDefinitionSubjectMatterExpertEntity.getUserId());
    }

    /**
     * Validates the business object definition subject matter expert create request. This method also trims the request parameters.
     *
     * @param request the business object definition subject matter expert create request
     */
    private void validateBusinessObjectDefinitionSubjectMatterExpertCreateRequest(BusinessObjectDefinitionSubjectMatterExpertCreateRequest request)
    {
        Assert.notNull(request, "A business object definition subject matter expert create request must be specified.");
        validateBusinessObjectDefinitionSubjectMatterExpertKey(request.getBusinessObjectDefinitionSubjectMatterExpertKey());
    }

    /**
     * Validates the business object definition subject matter expert key. This method also trims the key parameters.
     *
     * @param key the business object definition subject matter expert key
     */
    private void validateBusinessObjectDefinitionSubjectMatterExpertKey(BusinessObjectDefinitionSubjectMatterExpertKey key)
    {
        Assert.notNull(key, "A business object definition subject matter expert key must be specified.");
        key.setNamespace(alternateKeyHelper.validateStringParameter("namespace", key.getNamespace()));
        key.setBusinessObjectDefinitionName(
            alternateKeyHelper.validateStringParameter("business object definition name", key.getBusinessObjectDefinitionName()));
        key.setUserId(alternateKeyHelper.validateStringParameter("user id", key.getUserId()));
    }
}
