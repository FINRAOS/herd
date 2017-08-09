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

import org.finra.herd.core.HerdDateUtils;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.api.xml.SearchIndex;
import org.finra.herd.model.api.xml.SearchIndexActivation;
import org.finra.herd.model.api.xml.SearchIndexActivationCreateRequest;
import org.finra.herd.model.api.xml.SearchIndexKey;
import org.finra.herd.model.jpa.SearchIndexEntity;
import org.finra.herd.service.SearchIndexActivationService;
import org.finra.herd.service.helper.AlternateKeyHelper;
import org.finra.herd.service.helper.SearchIndexDaoHelper;

/**
 * The search index activation service implementation.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class SearchIndexActivationServiceImpl implements SearchIndexActivationService
{
    @Autowired
    private AlternateKeyHelper alternateKeyHelper;

    @Autowired
    private SearchIndexDaoHelper searchIndexDaoHelper;

    @Override
    public SearchIndexActivation createSearchIndexActivation(SearchIndexActivationCreateRequest request)
    {
        // Validate the request
        validateSearchIndexActivationRequest(request);

        // Ensure that search index for the specified search index key exists.
        SearchIndexEntity searchIndexEntity = searchIndexDaoHelper.getSearchIndexEntity(request.getSearchIndexKey());

        // Activate the specified search index entity.
        searchIndexDaoHelper.activateSearchIndex(searchIndexEntity);

        SearchIndex searchIndex = createSearchIndexFromEntity(searchIndexEntity);
        return new SearchIndexActivation(searchIndex.getSearchIndexKey(), searchIndex.getSearchIndexType(), searchIndex.getSearchIndexStatus(),
            searchIndex.isActive(), searchIndex.getCreatedByUserId(), searchIndex.getCreatedOn(), searchIndex.getLastUpdatedOn());
    }

    /**
     * Validates the search index activation create request. This method also trims request parameters.
     *
     * @param request the request
     *
     * @throws IllegalArgumentException if any activation errors were found
     */
    private void validateSearchIndexActivationRequest(SearchIndexActivationCreateRequest request) throws IllegalArgumentException
    {
        Assert.notNull(request, "A search index activation create request must be specified.");
        validateSearchIndexKey(request.getSearchIndexKey());
    }

    /**
     * Validates a search index key. This method trims the key parameters and ensures it exists in the database.
     *
     * @param searchIndexKey the search index key
     *
     * @throws IllegalArgumentException if any activation errors were found
     */
    private void validateSearchIndexKey(SearchIndexKey searchIndexKey) throws IllegalArgumentException
    {
        Assert.notNull(searchIndexKey, "A search index key must be specified.");
        searchIndexKey.setSearchIndexName(alternateKeyHelper.validateStringParameter("Search index name", searchIndexKey.getSearchIndexName()));
    }

    /**
     * Creates a search index object from the persisted entity.
     *
     * @param searchIndexEntity the search index entity
     *
     * @return the search index
     */
    protected SearchIndex createSearchIndexFromEntity(SearchIndexEntity searchIndexEntity)
    {
        SearchIndex searchIndex = new SearchIndex();
        searchIndex.setSearchIndexKey(new SearchIndexKey(searchIndexEntity.getName()));
        searchIndex.setSearchIndexType(searchIndexEntity.getType().getCode());
        searchIndex.setSearchIndexStatus(searchIndexEntity.getStatus().getCode());
        searchIndex.setActive(searchIndexEntity.getActive());
        searchIndex.setCreatedByUserId(searchIndexEntity.getCreatedBy());
        searchIndex.setCreatedOn(HerdDateUtils.getXMLGregorianCalendarValue(searchIndexEntity.getCreatedOn()));
        searchIndex.setLastUpdatedOn(HerdDateUtils.getXMLGregorianCalendarValue(searchIndexEntity.getUpdatedOn()));
        return searchIndex;
    }
}
