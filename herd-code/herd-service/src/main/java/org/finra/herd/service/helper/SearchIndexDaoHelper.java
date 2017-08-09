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

import java.util.List;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.dao.SearchIndexDao;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.SearchIndexKey;
import org.finra.herd.model.jpa.SearchIndexEntity;
import org.finra.herd.model.jpa.SearchIndexStatusEntity;
import org.finra.herd.model.jpa.SearchIndexTypeEntity;

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

    @Autowired
    private SearchIndexTypeDaoHelper searchIndexTypeDaoHelper;

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


    /**
     * Activates a search index entity and de-activates the others
     *
     * @param currentSearchIndexEntity the search index key (case sensitive)
     *
     * @throws org.finra.herd.model.ObjectNotFoundException if the search index entity
     */
    public void activateSearchIndex(SearchIndexEntity currentSearchIndexEntity)
    {

        // Get all the search index entities of the respective type.
        List<SearchIndexEntity> searchIndexEntities = searchIndexDao.getSearchIndexEntities(currentSearchIndexEntity.getType());

        // Exclude the current search index entity and de-activate the other entities.
        searchIndexEntities.stream().filter(searchIndexEntity -> !currentSearchIndexEntity.equals(searchIndexEntity)).collect(Collectors.toList())
            .forEach(searchIndexEntity -> {
                searchIndexEntity.setActive(Boolean.FALSE);
                searchIndexDao.saveAndRefresh(searchIndexEntity);
            });

        // Set the current search index entity to active.
        currentSearchIndexEntity.setActive(Boolean.TRUE);
        searchIndexDao.saveAndRefresh(currentSearchIndexEntity);
    }

    /**
     * Fetches the name of the active index for the specified type
     *
     * @param indexType the type of the search index
     */
    public String getActiveSearchIndex(String indexType)
    {

        // Get the search index type and ensure it exists.
        SearchIndexTypeEntity searchIndexTypeEntity = searchIndexTypeDaoHelper.getSearchIndexTypeEntity(indexType);

        // Fetch the list of all search index entities for the specified type and get the active entity
        List<SearchIndexEntity> searchIndexEntities = searchIndexDao.getSearchIndexEntities(searchIndexTypeEntity);

        searchIndexEntities = searchIndexEntities.stream()
            .filter(searchIndexEntity -> searchIndexEntity.getActive() != null && searchIndexEntity.getActive().equals(Boolean.TRUE))
            .collect(Collectors.toList());

        if (searchIndexEntities.size() == 0)
        {
            throw new ObjectNotFoundException(String.format("No active search index found for type \"%s\".", indexType));
        }

        return searchIndexEntities.get(0).getName();
    }



}
