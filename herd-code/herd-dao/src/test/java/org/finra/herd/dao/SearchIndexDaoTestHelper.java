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
package org.finra.herd.dao;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.model.jpa.SearchIndexEntity;

@Component
public class SearchIndexDaoTestHelper
{
    @Autowired
    private SearchIndexDao searchIndexDao;

    @Autowired
    private SearchIndexStatusDaoTestHelper searchIndexStatusDaoTestHelper;

    @Autowired
    private SearchIndexTypeDaoTestHelper searchIndexTypeDaoTestHelper;

    /**
     * Creates and persists a new search index entity.
     *
     * @param searchIndexName the name of the search index
     * @param typeCode the code of the search index type
     * @param statusCode the code of the search index status
     *
     * @return the newly created search index entity
     */
    public SearchIndexEntity createSearchIndexEntity(String searchIndexName, String typeCode, String statusCode)
    {
        SearchIndexEntity searchIndexEntity = new SearchIndexEntity();
        searchIndexEntity.setName(searchIndexName);
        searchIndexEntity.setType(searchIndexTypeDaoTestHelper.createSearchIndexTypeEntity(typeCode));
        searchIndexEntity.setStatus(searchIndexStatusDaoTestHelper.createSearchIndexStatusEntity(statusCode));
        return searchIndexDao.saveAndRefresh(searchIndexEntity);
    }
}
