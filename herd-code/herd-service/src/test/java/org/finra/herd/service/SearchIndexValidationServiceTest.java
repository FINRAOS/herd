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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.scheduling.annotation.AsyncResult;

import org.finra.herd.model.api.xml.SearchIndexKey;
import org.finra.herd.model.api.xml.SearchIndexValidation;
import org.finra.herd.model.api.xml.SearchIndexValidationCreateRequest;
import org.finra.herd.model.jpa.SearchIndexEntity;
import org.finra.herd.model.jpa.SearchIndexTypeEntity;
import org.finra.herd.service.helper.AlternateKeyHelper;
import org.finra.herd.service.helper.SearchIndexDaoHelper;
import org.finra.herd.service.impl.SearchIndexValidationServiceImpl;

/**
 * This class tests search index validation functionality within the search index validation service.
 */
public class SearchIndexValidationServiceTest extends AbstractServiceTest
{
    @InjectMocks
    private SearchIndexValidationServiceImpl searchIndexValidationService;

    @Mock
    private AlternateKeyHelper alternateKeyHelper;

    @Mock
    private SearchIndexDaoHelper searchIndexDaoHelper;

    @Mock
    private BusinessObjectDefinitionService businessObjectDefinitionService;

    @Mock
    private TagService tagService;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    private void searchIndexValidation(SearchIndexKey searchIndexKey, SearchIndexValidationCreateRequest searchIndexValidationCreateRequest,
        String searchIndexType)
    {
        //Create the search index type entity
        SearchIndexTypeEntity searchIndexTypeEntity = new SearchIndexTypeEntity();
        searchIndexTypeEntity.setCode(searchIndexType);

        //Create a search index entity
        SearchIndexEntity searchIndexEntity = new SearchIndexEntity();
        searchIndexEntity.setName(SEARCH_INDEX_NAME);
        searchIndexEntity.setType(searchIndexTypeEntity);

        //validation response
        boolean sizeCheck = ThreadLocalRandom.current().nextDouble() < 0.5;
        boolean spotCheckPercentage = ThreadLocalRandom.current().nextDouble() < 0.5;
        boolean spotCheckMostRecent = ThreadLocalRandom.current().nextDouble() < 0.5;

        // Mock some of the external call responses.
        @SuppressWarnings("unchecked")
        Future<Void> mockedFuture = mock(Future.class);

        // Mock the external calls.
        when(alternateKeyHelper.validateStringParameter("Search index name", SEARCH_INDEX_NAME)).thenReturn(SEARCH_INDEX_NAME);
        when(searchIndexDaoHelper.getSearchIndexEntity(searchIndexKey)).thenReturn(searchIndexEntity);
        when(businessObjectDefinitionService.indexValidateAllBusinessObjectDefinitions()).thenReturn(new AsyncResult<>(null));
        when(businessObjectDefinitionService.indexSizeCheckValidationBusinessObjectDefinitions()).thenReturn(sizeCheck);
        when(businessObjectDefinitionService.indexSpotCheckPercentageValidationBusinessObjectDefinitions()).thenReturn(spotCheckPercentage);
        when(businessObjectDefinitionService.indexSpotCheckMostRecentValidationBusinessObjectDefinitions()).thenReturn(spotCheckMostRecent);

        when(tagService.indexValidateAllTags()).thenReturn(new AsyncResult<>(null));
        when(tagService.indexSizeCheckValidationTags()).thenReturn(sizeCheck);
        when(tagService.indexSpotCheckPercentageValidationTags()).thenReturn(spotCheckPercentage);
        when(tagService.indexSpotCheckMostRecentValidationTags()).thenReturn(spotCheckMostRecent);

        // Create a search index.
        SearchIndexValidation response = searchIndexValidationService.createSearchIndexValidation(searchIndexValidationCreateRequest);

        // Verify the external calls.
        verify(alternateKeyHelper).validateStringParameter("Search index name", SEARCH_INDEX_NAME);
        verify(searchIndexDaoHelper).getSearchIndexEntity(searchIndexKey);

        if (searchIndexType.equals(SearchIndexTypeEntity.SearchIndexTypes.TAG.name()))
        {
            // verify that full validation is invoked only if specified in the request
            if (searchIndexValidationCreateRequest.isFull())
            {
                verify(tagService, times(1)).indexValidateAllTags();
            }
            else
            {
                verify(tagService, times(0)).indexValidateAllTags();
            }
            verify(tagService).indexSizeCheckValidationTags();
            verify(tagService).indexSpotCheckPercentageValidationTags();
            verify(tagService).indexSpotCheckMostRecentValidationTags();
            verifyNoMoreInteractions(alternateKeyHelper, searchIndexDaoHelper, tagService);
            // Validate the returned object.
            assertEquals(new SearchIndexValidation(searchIndexKey, response.getValidateStartTime(), sizeCheck, spotCheckPercentage, spotCheckMostRecent),
                response);
        }

        else
        {
            // verify that full validation is invoked only if specified in the request
            if (searchIndexValidationCreateRequest.isFull())
            {
                verify(businessObjectDefinitionService, times(1)).indexValidateAllBusinessObjectDefinitions();
            }
            else
            {
                verify(businessObjectDefinitionService, times(0)).indexValidateAllBusinessObjectDefinitions();
            }
            verify(businessObjectDefinitionService).indexSizeCheckValidationBusinessObjectDefinitions();
            verify(businessObjectDefinitionService).indexSpotCheckPercentageValidationBusinessObjectDefinitions();
            verify(businessObjectDefinitionService).indexSpotCheckMostRecentValidationBusinessObjectDefinitions();
            verifyNoMoreInteractions(alternateKeyHelper, searchIndexDaoHelper, businessObjectDefinitionService);
            // Validate the returned object.
            assertEquals(new SearchIndexValidation(searchIndexKey, response.getValidateStartTime(), sizeCheck, spotCheckPercentage, spotCheckMostRecent),
                response);
        }

    }

    @Test
    public void testCreateSearchIndexValidationBusinessObjectDefinitionsFull()
    {
        // Create a search index key.
        SearchIndexKey searchIndexKey = new SearchIndexKey(SEARCH_INDEX_NAME);

        // Get the search index type value.
        String searchIndexType = SearchIndexTypeEntity.SearchIndexTypes.BUS_OBJCT_DFNTN.name();

        // Create a search index create request.
        SearchIndexValidationCreateRequest searchIndexValidationCreateRequest =
            new SearchIndexValidationCreateRequest(searchIndexKey, SEARCH_INDEX_VALIDATION_ENABLED);

        searchIndexValidation(searchIndexKey, searchIndexValidationCreateRequest, searchIndexType);
    }

    @Test
    public void testCreateSearchIndexValidationBusinessObjectDefinitions()
    {
        // Create a search index key.
        SearchIndexKey searchIndexKey = new SearchIndexKey(SEARCH_INDEX_NAME);

        // Get the search index type value.
        String searchIndexType = SearchIndexTypeEntity.SearchIndexTypes.BUS_OBJCT_DFNTN.name();

        // Create a search index create request.
        SearchIndexValidationCreateRequest searchIndexValidationCreateRequest =
            new SearchIndexValidationCreateRequest(searchIndexKey, SEARCH_INDEX_VALIDATION_DISABLED);

        searchIndexValidation(searchIndexKey, searchIndexValidationCreateRequest, searchIndexType);
    }

    @Test
    public void testCreateSearchIndexValidationTagsFull()
    {
        // Create a search index key.
        SearchIndexKey searchIndexKey = new SearchIndexKey(SEARCH_INDEX_NAME);

        // Get the search index type value.
        String searchIndexType = SearchIndexTypeEntity.SearchIndexTypes.TAG.name();

        // Create a search index create request.
        SearchIndexValidationCreateRequest searchIndexValidationCreateRequest =
            new SearchIndexValidationCreateRequest(searchIndexKey, SEARCH_INDEX_VALIDATION_ENABLED);

        searchIndexValidation(searchIndexKey, searchIndexValidationCreateRequest, searchIndexType);
    }

    @Test
    public void testCreateSearchIndexValidationTags()
    {
        // Create a search index key.
        SearchIndexKey searchIndexKey = new SearchIndexKey(SEARCH_INDEX_NAME);

        // Get the search index type value.
        String searchIndexType = SearchIndexTypeEntity.SearchIndexTypes.TAG.name();

        // Create a search index create request.
        SearchIndexValidationCreateRequest searchIndexValidationCreateRequest =
            new SearchIndexValidationCreateRequest(searchIndexKey, SEARCH_INDEX_VALIDATION_DISABLED);

        searchIndexValidation(searchIndexKey, searchIndexValidationCreateRequest, searchIndexType);
    }
}
