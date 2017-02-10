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
import org.finra.herd.model.jpa.SearchIndexTypeEntity;
import org.finra.herd.service.AbstractServiceTest;

/**
 * This class tests functionality within the relative helper class.
 */
public class SearchIndexTypeDaoHelperTest extends AbstractServiceTest
{
    @Test
    public void testGetSearchIndexTypeEntity()
    {
        // Create and persist database entities required for testing.
        SearchIndexTypeEntity searchIndexTypeEntity = searchIndexTypeDaoTestHelper.createSearchIndexTypeEntity(SEARCH_INDEX_TYPE);

        // Retrieve the search index type entity.
        assertEquals(searchIndexTypeEntity, searchIndexTypeDaoHelper.getSearchIndexTypeEntity(SEARCH_INDEX_TYPE));

        // Test case insensitivity of the search index type code.
        assertEquals(searchIndexTypeEntity, searchIndexTypeDaoHelper.getSearchIndexTypeEntity(SEARCH_INDEX_TYPE.toUpperCase()));
        assertEquals(searchIndexTypeEntity, searchIndexTypeDaoHelper.getSearchIndexTypeEntity(SEARCH_INDEX_TYPE.toLowerCase()));

        // Try to retrieve a non existing search index type.
        try
        {
            searchIndexTypeDaoHelper.getSearchIndexTypeEntity(I_DO_NOT_EXIST);
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Search index type with code \"%s\" doesn't exist.", I_DO_NOT_EXIST), e.getMessage());
        }
    }
}
