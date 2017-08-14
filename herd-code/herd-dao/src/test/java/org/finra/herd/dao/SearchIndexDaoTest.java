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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Arrays;

import org.junit.Test;

import org.finra.herd.model.api.xml.SearchIndexKey;
import org.finra.herd.model.jpa.SearchIndexEntity;

public class SearchIndexDaoTest extends AbstractDaoTest
{
    @Test
    public void testGetSearchIndexByKey()
    {
        // Create database entities required for testing.
        SearchIndexEntity searchIndexEntity = searchIndexDaoTestHelper.createSearchIndexEntity(SEARCH_INDEX_NAME, SEARCH_INDEX_TYPE, SEARCH_INDEX_STATUS);

        // Retrieve the search index entity and validate the results.
        assertEquals(searchIndexEntity, searchIndexDao.getSearchIndexByKey(new SearchIndexKey(SEARCH_INDEX_NAME)));

        // Test case sensitivity for the search index name.
        assertNull(searchIndexDao.getSearchIndexByKey(new SearchIndexKey(SEARCH_INDEX_NAME.toUpperCase())));
        assertNull(searchIndexDao.getSearchIndexByKey(new SearchIndexKey(SEARCH_INDEX_NAME.toLowerCase())));

        // Confirm negative results when using non-existing search index name.
        assertNull(searchIndexDao.getSearchIndexByKey(new SearchIndexKey("I_DO_NOT_EXIST")));
    }

    @Test
    public void testGetSearchIndexes()
    {
        // Create database entities required for testing in reverse order to validate the order by clause.
        searchIndexDaoTestHelper.createSearchIndexEntity(SEARCH_INDEX_NAME_2, SEARCH_INDEX_TYPE_2, SEARCH_INDEX_STATUS_2);
        searchIndexDaoTestHelper.createSearchIndexEntity(SEARCH_INDEX_NAME, SEARCH_INDEX_TYPE, SEARCH_INDEX_STATUS);

        // Retrieve the search index keys and validate the results.
        assertEquals(Arrays.asList(new SearchIndexKey(SEARCH_INDEX_NAME), new SearchIndexKey(SEARCH_INDEX_NAME_2)), searchIndexDao.getSearchIndexes());
    }


    @Test
    public void testGetSearchIndexEntities()
    {
        // Create database entities required for testing
        SearchIndexEntity searchIndexEntity = searchIndexDaoTestHelper.createSearchIndexEntity(SEARCH_INDEX_NAME, SEARCH_INDEX_TYPE, SEARCH_INDEX_STATUS);
        SearchIndexEntity searchIndexEntity1 =
            searchIndexDaoTestHelper.createSearchIndexEntity(SEARCH_INDEX_NAME_2, SEARCH_INDEX_TYPE_2, SEARCH_INDEX_STATUS_2);

        // Retrieve the search index keys and validate the results.
        assertEquals(Arrays.asList(searchIndexEntity), searchIndexDao.getSearchIndexEntities(searchIndexEntity.getType()));
        assertEquals(Arrays.asList(searchIndexEntity1), searchIndexDao.getSearchIndexEntities(searchIndexEntity1.getType()));
    }
}
