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

import org.finra.herd.dao.SearchIndexStatusDao;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.jpa.SearchIndexStatusEntity;

/**
 * Helper for search index status operations which require DAO.
 */
@Component
public class SearchIndexStatusDaoHelper
{
    @Autowired
    private SearchIndexStatusDao searchIndexStatusDao;

    /**
     * Gets a search index status entity by its code and ensure it exists.
     *
     * @param code the search index status code (case insensitive)
     *
     * @return the search index status entity
     * @throws org.finra.herd.model.ObjectNotFoundException if the search index status entity doesn't exist
     */
    public SearchIndexStatusEntity getSearchIndexStatusEntity(String code) throws ObjectNotFoundException
    {
        SearchIndexStatusEntity searchIndexStatusEntity = searchIndexStatusDao.getSearchIndexStatusByCode(code);

        if (searchIndexStatusEntity == null)
        {
            throw new ObjectNotFoundException(String.format("Search index status with code \"%s\" doesn't exist.", code));
        }

        return searchIndexStatusEntity;
    }
}
