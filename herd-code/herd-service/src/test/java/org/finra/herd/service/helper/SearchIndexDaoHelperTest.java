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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Test;

import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.SearchIndexKey;
import org.finra.herd.model.jpa.SearchIndexEntity;
import org.finra.herd.service.AbstractServiceTest;

/**
 * This class tests functionality within the relative helper class.
 */
public class SearchIndexDaoHelperTest extends AbstractServiceTest
{
    @Test
    public void testGetSearchIndexEntity()
    {
        // Create and persist database entities required for testing.
        SearchIndexEntity searchIndexEntity = searchIndexDaoTestHelper.createSearchIndexEntity(SEARCH_INDEX_NAME, SEARCH_INDEX_TYPE, SEARCH_INDEX_STATUS);

        // Retrieve the search index entity.
        assertEquals(searchIndexEntity, searchIndexDaoHelper.getSearchIndexEntity(new SearchIndexKey(SEARCH_INDEX_NAME)));

        // Try to retrieve a non-existing search index.
        try
        {
            searchIndexDaoHelper.getSearchIndexEntity(new SearchIndexKey(I_DO_NOT_EXIST));
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Search index with name \"%s\" doesn't exist.", I_DO_NOT_EXIST), e.getMessage());
        }
    }

    @Test
    public void testUpdateSearchIndexStatus()
    {
        // Create and persist a search index entity.
        SearchIndexEntity searchIndexEntity = searchIndexDaoTestHelper.createSearchIndexEntity(SEARCH_INDEX_NAME, SEARCH_INDEX_TYPE, SEARCH_INDEX_STATUS);

        // Create and persist a search index status entity.
        searchIndexStatusDaoTestHelper.createSearchIndexStatusEntity(SEARCH_INDEX_STATUS_2);

        // Update the status of the search index entity.
        searchIndexDaoHelper.updateSearchIndexStatus(new SearchIndexKey(SEARCH_INDEX_NAME), SEARCH_INDEX_STATUS_2);

        // Validate the results.
        assertEquals(SEARCH_INDEX_STATUS_2, searchIndexEntity.getStatus().getCode());
    }
}
