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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.dao.SearchIndexDao;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.SearchIndexKey;
import org.finra.herd.model.jpa.SearchIndexEntity;
import org.finra.herd.model.jpa.SearchIndexStatusEntity;

/**
 * Helper for search index related operations which require DAO.
 */
@Component
public class SearchIndexDaoHelper
{
    @Autowired
    private SearchIndexDao searchIndexDao;

    @Autowired
    private SearchIndexStatusDaoHelper searchIndexStatusDaoHelper;

    /**
     * Gets a search index entity by its key and ensure it exists.
     *
     * @param searchIndexKey the search index key (case sensitive)
     *
     * @return the search index entity
     * @throws org.finra.herd.model.ObjectNotFoundException if the search index entity doesn't exist
     */
    public SearchIndexEntity getSearchIndexEntity(SearchIndexKey searchIndexKey) throws ObjectNotFoundException
    {
        SearchIndexEntity searchIndexEntity = searchIndexDao.getSearchIndexByKey(searchIndexKey);

        if (searchIndexEntity == null)
        {
            throw new ObjectNotFoundException(String.format("Search index with name \"%s\" doesn't exist.", searchIndexKey.getSearchIndexName()));
        }

        return searchIndexEntity;
    }

    /**
     * Gets a search index entity by its key and ensure it exists.
     *
     * @param searchIndexKey the search index key (case sensitive)
     * @param searchIndexStatus the status of the search index (case insensitive)
     *
     * @throws org.finra.herd.model.ObjectNotFoundException if the search index entity or search index status entity doesn't exist
     */
    public void updateSearchIndexStatus(SearchIndexKey searchIndexKey, String searchIndexStatus) throws ObjectNotFoundException
    {
        // Get the search index entity and ensure that it exists.
        SearchIndexEntity searchIndexEntity = getSearchIndexEntity(searchIndexKey);

        // Get the search index status entity and ensure that it exists.
        SearchIndexStatusEntity searchIndexStatusEntity = searchIndexStatusDaoHelper.getSearchIndexStatusEntity(searchIndexStatus);

        // Update the search index status.
        searchIndexEntity.setStatus(searchIndexStatusEntity);

        // Persist the entity.
        searchIndexDao.saveAndRefresh(searchIndexEntity);
    }
}
