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

import static org.finra.herd.core.HerdDateUtils.getXMLGregorianCalendarValue;

import java.util.Date;

import org.apache.commons.lang.BooleanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.api.xml.SearchIndexKey;
import org.finra.herd.model.api.xml.SearchIndexValidation;
import org.finra.herd.model.api.xml.SearchIndexValidationCreateRequest;
import org.finra.herd.model.jpa.SearchIndexEntity;
import org.finra.herd.model.jpa.SearchIndexTypeEntity;
import org.finra.herd.service.BusinessObjectDefinitionService;
import org.finra.herd.service.SearchIndexValidationService;
import org.finra.herd.service.TagService;
import org.finra.herd.service.helper.AlternateKeyHelper;
import org.finra.herd.service.helper.SearchIndexDaoHelper;

/**
 * The search index validation service implementation.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class SearchIndexValidationServiceImpl implements SearchIndexValidationService
{
    @Autowired
    private AlternateKeyHelper alternateKeyHelper;

    @Autowired
    private SearchIndexDaoHelper searchIndexDaoHelper;

    @Autowired
    private BusinessObjectDefinitionService businessObjectDefinitionService;

    @Autowired
    private TagService tagService;

    @Override
    public SearchIndexValidation createSearchIndexValidation(SearchIndexValidationCreateRequest request)
    {
        //validate the request
        validateSearchIndexValidationRequest(request);

        // Ensure that search index for the specified search index key exists. Fetch the type
        SearchIndexEntity searchIndexEntity = searchIndexDaoHelper.getSearchIndexEntity(request.getSearchIndexKey());
        String searchIndexType = searchIndexEntity.getType().getCode();

        boolean sizeCheck = false;
        boolean spotCheckPercentage = false;
        boolean spotCheckMostRecent = false;

        // Currently, only search index for business object definitions and tag are supported.
        if (SearchIndexTypeEntity.SearchIndexTypes.BUS_OBJCT_DFNTN.name().equalsIgnoreCase(searchIndexType))
        {
            // only perform full validation if specified in the request
            if (BooleanUtils.isTrue(request.isFull()))
            {
                businessObjectDefinitionService.indexValidateAllBusinessObjectDefinitions();
            }
            sizeCheck = businessObjectDefinitionService.indexSizeCheckValidationBusinessObjectDefinitions();
            spotCheckPercentage = businessObjectDefinitionService.indexSpotCheckPercentageValidationBusinessObjectDefinitions();
            spotCheckMostRecent = businessObjectDefinitionService.indexSpotCheckMostRecentValidationBusinessObjectDefinitions();
        }
        else if (SearchIndexTypeEntity.SearchIndexTypes.TAG.name().equalsIgnoreCase(searchIndexType))
        {
            // only perform full validation if specified in the request
            if (BooleanUtils.isTrue(request.isFull()))
            {
                tagService.indexValidateAllTags();
            }
            sizeCheck = tagService.indexSizeCheckValidationTags();
            spotCheckPercentage = tagService.indexSpotCheckPercentageValidationTags();
            spotCheckMostRecent = tagService.indexSpotCheckMostRecentValidationTags();
        }

        return new SearchIndexValidation(request.getSearchIndexKey(), getXMLGregorianCalendarValue(new Date()), sizeCheck, spotCheckPercentage,
            spotCheckMostRecent);
    }

    /**
     * Validates the search index validation create request. This method also trims request parameters.
     *
     * @param request the request
     *
     * @throws IllegalArgumentException if any validation errors were found
     */
    private void validateSearchIndexValidationRequest(SearchIndexValidationCreateRequest request) throws IllegalArgumentException
    {
        Assert.notNull(request, "A search create request must be specified.");
        validateSearchIndexKey(request.getSearchIndexKey());
    }

    /**
     * Validates a search index key. This method trims the key parameters and ensures it exists in the database.
     *
     * @param searchIndexKey the search index key
     *
     * @throws IllegalArgumentException if any validation errors were found
     */
    private void validateSearchIndexKey(SearchIndexKey searchIndexKey) throws IllegalArgumentException
    {
        Assert.notNull(searchIndexKey, "A search index key must be specified.");
        searchIndexKey.setSearchIndexName(alternateKeyHelper.validateStringParameter("Search index name", searchIndexKey.getSearchIndexName()));
    }
}
