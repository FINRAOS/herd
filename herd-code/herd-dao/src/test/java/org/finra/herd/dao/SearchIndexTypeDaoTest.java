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

import org.junit.Test;

public class SearchIndexTypeDaoTest extends AbstractDaoTest
{
    @Test
    public void testGetSearchIndexTypeByCode()
    {
        // Create database entities required for testing.
        searchIndexTypeDaoTestHelper.createSearchIndexTypeEntity(SEARCH_INDEX_TYPE);
        searchIndexTypeDaoTestHelper.createSearchIndexTypeEntity(SEARCH_INDEX_TYPE_2);

        // Retrieve the relative search index type entities and validate the results.
        assertEquals(SEARCH_INDEX_TYPE, searchIndexTypeDao.getSearchIndexTypeByCode(SEARCH_INDEX_TYPE).getCode());
        assertEquals(SEARCH_INDEX_TYPE_2, searchIndexTypeDao.getSearchIndexTypeByCode(SEARCH_INDEX_TYPE_2).getCode());

        // Test case insensitivity for the search index type code.
        assertEquals(SEARCH_INDEX_TYPE, searchIndexTypeDao.getSearchIndexTypeByCode(SEARCH_INDEX_TYPE.toUpperCase()).getCode());
        assertEquals(SEARCH_INDEX_TYPE, searchIndexTypeDao.getSearchIndexTypeByCode(SEARCH_INDEX_TYPE.toLowerCase()).getCode());

        // Confirm negative results when using non-existing search index type code.
        assertNull(searchIndexTypeDao.getSearchIndexTypeByCode("I_DO_NOT_EXIST"));
    }
}
