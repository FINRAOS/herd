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
import org.finra.herd.model.jpa.SearchIndexStatusEntity;
import org.finra.herd.service.AbstractServiceTest;

/**
 * This class tests functionality within the relative helper class.
 */
public class SearchIndexStatusDaoHelperTest extends AbstractServiceTest
{
    @Test
    public void testGetSearchIndexStatusEntity()
    {
        // Create and persist database entities required for testing.
        SearchIndexStatusEntity searchIndexStatusEntity = searchIndexStatusDaoTestHelper.createSearchIndexStatusEntity(SEARCH_INDEX_STATUS);

        // Retrieve the search index status entity.
        assertEquals(searchIndexStatusEntity, searchIndexStatusDaoHelper.getSearchIndexStatusEntity(SEARCH_INDEX_STATUS));

        // Test case insensitivity of the search index status code.
        assertEquals(searchIndexStatusEntity, searchIndexStatusDaoHelper.getSearchIndexStatusEntity(SEARCH_INDEX_STATUS.toUpperCase()));
        assertEquals(searchIndexStatusEntity, searchIndexStatusDaoHelper.getSearchIndexStatusEntity(SEARCH_INDEX_STATUS.toLowerCase()));

        // Try to retrieve a non existing search index status.
        try
        {
            searchIndexStatusDaoHelper.getSearchIndexStatusEntity(I_DO_NOT_EXIST);
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Search index status with code \"%s\" doesn't exist.", I_DO_NOT_EXIST), e.getMessage());
        }
    }
}
