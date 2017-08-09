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
package org.finra.herd.service;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.sql.Timestamp;
import java.util.Arrays;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.model.api.xml.SearchIndex;
import org.finra.herd.model.api.xml.SearchIndexActivation;
import org.finra.herd.model.api.xml.SearchIndexActivationCreateRequest;
import org.finra.herd.model.api.xml.SearchIndexKey;
import org.finra.herd.model.jpa.SearchIndexEntity;
import org.finra.herd.model.jpa.SearchIndexStatusEntity;
import org.finra.herd.model.jpa.SearchIndexTypeEntity;
import org.finra.herd.service.helper.AlternateKeyHelper;
import org.finra.herd.service.helper.SearchIndexDaoHelper;
import org.finra.herd.service.impl.SearchIndexActivationServiceImpl;

/**
 * This class tests search index activation functionality within the search index activation service.
 */
public class SearchIndexActivationServiceTest extends AbstractServiceTest
{
    @InjectMocks
    private SearchIndexActivationServiceImpl searchIndexActivationService;

    @Mock
    private AlternateKeyHelper alternateKeyHelper;

    @Mock
    private SearchIndexDaoHelper searchIndexDaoHelper;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    @Ignore
    public void testSearchIndexActivation()
    {
        // Get the search index type value.
        String searchIndexType = SearchIndexTypeEntity.SearchIndexTypes.BUS_OBJCT_DFNTN.name();
        SearchIndexTypeEntity searchIndexTypeEntity = new SearchIndexTypeEntity();
        searchIndexTypeEntity.setCode(searchIndexType);

        // Get the search index status value.
        String searchIndexStatus = SearchIndexStatusEntity.SearchIndexStatuses.READY.name();

        // Creates a test search index status entity.
        SearchIndexStatusEntity searchIndexStatusEntity = new SearchIndexStatusEntity();
        searchIndexStatusEntity.setCode(searchIndexStatus);

        // Create a search index entity
        SearchIndexEntity searchIndexEntity = searchIndexDaoTestHelper.createSearchIndexEntity(SEARCH_INDEX_NAME, searchIndexType, searchIndexStatus);

        // Search index key
        SearchIndexKey searchIndexKey = new SearchIndexKey(SEARCH_INDEX_NAME);

        // Create a search index entity response
        SearchIndexEntity responseSearchIndexEntity = new SearchIndexEntity();
        responseSearchIndexEntity.setName(SEARCH_INDEX_NAME);
        responseSearchIndexEntity.setType(searchIndexTypeEntity);
        responseSearchIndexEntity.setActive(Boolean.TRUE);
        responseSearchIndexEntity.setCreatedBy(USER_ID);
        responseSearchIndexEntity.setCreatedOn(new Timestamp(CREATED_ON.toGregorianCalendar().getTimeInMillis()));
        responseSearchIndexEntity.setUpdatedOn(new Timestamp(UPDATED_ON.toGregorianCalendar().getTimeInMillis()));


        // Create a search index activation request.
        SearchIndexActivationCreateRequest searchIndexActivationCreateRequest = new SearchIndexActivationCreateRequest();
        searchIndexActivationCreateRequest.setSearchIndexKey(searchIndexKey);

        // Mock the external calls.
        when(alternateKeyHelper.validateStringParameter("Search index name", SEARCH_INDEX_NAME)).thenReturn(SEARCH_INDEX_NAME);
        when(searchIndexDaoHelper.getSearchIndexEntity(searchIndexKey)).thenReturn(searchIndexEntity);
        when(searchIndexDao.getSearchIndexEntities(searchIndexTypeEntity)).thenReturn(Arrays.asList(searchIndexEntity));
        when(searchIndexDao.saveAndRefresh(any(SearchIndexEntity.class))).thenReturn(responseSearchIndexEntity);


        // Call the method under test.
        SearchIndexActivation response = searchIndexActivationService.createSearchIndexActivation(searchIndexActivationCreateRequest);

        // Verify the external calls.
        verify(alternateKeyHelper).validateStringParameter("Search index name", SEARCH_INDEX_NAME);
        verify(searchIndexDaoHelper).getSearchIndexEntity(searchIndexKey);
        verify(searchIndexDao).getSearchIndexEntities(searchIndexTypeEntity);
        verify(searchIndexDao).saveAndRefresh(any(SearchIndexEntity.class));

        verifyNoMoreInteractions(alternateKeyHelper, searchIndexDao, searchIndexDaoHelper);

        // Validate the returned object.
        assertEquals(
            new SearchIndex(searchIndexKey, searchIndexType, searchIndexStatus, SEARCH_INDEX_ACTIVE_FLAG, NO_SEARCH_INDEX_STATISTICS, USER_ID, CREATED_ON,
                UPDATED_ON), response);
    }


}
