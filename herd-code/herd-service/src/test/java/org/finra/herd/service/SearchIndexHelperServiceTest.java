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

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Future;

import org.elasticsearch.client.transport.TransportClient;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.dao.BusinessObjectDefinitionDao;
import org.finra.herd.dao.TagDao;
import org.finra.herd.model.api.xml.SearchIndexKey;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.model.jpa.SearchIndexStatusEntity;
import org.finra.herd.model.jpa.TagEntity;
import org.finra.herd.service.functional.SearchFunctions;
import org.finra.herd.service.helper.BusinessObjectDefinitionHelper;
import org.finra.herd.service.helper.SearchIndexDaoHelper;
import org.finra.herd.service.helper.TagHelper;
import org.finra.herd.service.impl.SearchIndexHelperServiceImpl;

/**
 * This class tests functionality within the search index helper service.
 */
public class SearchIndexHelperServiceTest extends AbstractServiceTest
{
    @Mock
    private BusinessObjectDefinitionDao businessObjectDefinitionDao;

    @Mock
    private BusinessObjectDefinitionHelper businessObjectDefinitionHelper;

    @Mock
    private SearchFunctions searchFunctions;

    @Mock
    private SearchIndexDaoHelper searchIndexDaoHelper;

    @InjectMocks
    private SearchIndexHelperServiceImpl searchIndexHelperService;

    @Mock
    private TagDao tagDao;

    @Mock
    private TagHelper tagHelper;

    @Mock
    private TransportClient transportClient;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testIndexAllBusinessObjectDefinitions()
    {
        // Create a search index key.
        SearchIndexKey searchIndexKey = new SearchIndexKey(SEARCH_INDEX_NAME);

        // Create a list of business object definition entities.
        final List<BusinessObjectDefinitionEntity> businessObjectDefinitionEntities = Collections.unmodifiableList(Arrays.asList(
            businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(BDEF_NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION,
                businessObjectDefinitionServiceTestHelper.getNewAttributes()), businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(BDEF_NAMESPACE_2, BDEF_NAME_2, DATA_PROVIDER_NAME_2, BDEF_DESCRIPTION_2,
                businessObjectDefinitionServiceTestHelper.getNewAttributes2())));

        // Get a chunk size.
        int chunkSize = SearchIndexHelperServiceImpl.BUSINESS_OBJECT_DEFINITIONS_CHUNK_SIZE;

        // Mock the external calls. Please note that we mock index size is set to be equal to the business object definition entity list size.
        when(businessObjectDefinitionDao.getAllBusinessObjectDefinitions(0, chunkSize)).thenReturn(businessObjectDefinitionEntities);
        when(businessObjectDefinitionDao.getAllBusinessObjectDefinitions(chunkSize, chunkSize)).thenReturn(new ArrayList<>());
        when(searchFunctions.getIndexFunction()).thenReturn((indexName, documentType, id, json) -> {
        });
        when(searchFunctions.getNumberOfTypesInIndexFunction()).thenReturn((indexName, documentType) -> 2L);

        // Index all business object definitions defined in the system.
        Future<Void> response = searchIndexHelperService.indexAllBusinessObjectDefinitions(searchIndexKey, SEARCH_INDEX_DOCUMENT_TYPE);

        // Verify the external calls.
        verify(businessObjectDefinitionDao).getAllBusinessObjectDefinitions(0, chunkSize);
        verify(businessObjectDefinitionDao).getAllBusinessObjectDefinitions(chunkSize, chunkSize);
        verify(searchFunctions).getIndexFunction();
        verify(businessObjectDefinitionHelper)
            .executeFunctionForBusinessObjectDefinitionEntities(eq(SEARCH_INDEX_NAME), eq(SEARCH_INDEX_DOCUMENT_TYPE), eq(businessObjectDefinitionEntities),
                any());
        verify(searchFunctions).getNumberOfTypesInIndexFunction();
        verify(searchIndexDaoHelper).updateSearchIndexStatus(searchIndexKey, SearchIndexStatusEntity.SearchIndexStatuses.READY.name());
        verifyNoMoreInteractions(businessObjectDefinitionDao, businessObjectDefinitionHelper, searchFunctions, searchIndexDaoHelper, tagDao, tagHelper,
            transportClient);

        // Validate the results.
        assertNotNull(response);
        assertThat(response, instanceOf(Future.class));
    }

    @Test
    public void testIndexAllTags()
    {
        // Create a search index key.
        SearchIndexKey searchIndexKey = new SearchIndexKey(SEARCH_INDEX_NAME);

        // Create a list of tag entities.
        final List<TagEntity> tagEntities = Collections.unmodifiableList(Arrays
            .asList(tagDaoTestHelper.createTagEntity(TAG_TYPE, TAG_CODE, TAG_DISPLAY_NAME, TAG_DESCRIPTION),
                tagDaoTestHelper.createTagEntity(TAG_TYPE_2, TAG_CODE_2, TAG_DISPLAY_NAME_2, TAG_DESCRIPTION_2)));

        // Mock the external calls. Please note that we mock index size is set to be equal to the tag entity list size.
        when(tagDao.getTags()).thenReturn(tagEntities);
        when(searchFunctions.getIndexFunction()).thenReturn((indexName, documentType, id, json) -> {
        });
        when(searchFunctions.getNumberOfTypesInIndexFunction()).thenReturn((indexName, documentType) -> 2L);

        // Index all tags defined in the system.
        Future<Void> response = searchIndexHelperService.indexAllTags(searchIndexKey, SEARCH_INDEX_DOCUMENT_TYPE);

        // Verify the external calls.
        verify(tagDao).getTags();
        verify(searchFunctions).getIndexFunction();
        verify(tagHelper).executeFunctionForTagEntities(eq(SEARCH_INDEX_NAME), eq(SEARCH_INDEX_DOCUMENT_TYPE), eq(tagEntities), any());
        verify(searchFunctions).getNumberOfTypesInIndexFunction();
        verify(searchIndexDaoHelper).updateSearchIndexStatus(searchIndexKey, SearchIndexStatusEntity.SearchIndexStatuses.READY.name());
        verifyNoMoreInteractions(businessObjectDefinitionDao, businessObjectDefinitionHelper, searchFunctions, searchIndexDaoHelper, tagDao, tagHelper,
            transportClient);

        // Validate the results.
        assertNotNull(response);
        assertThat(response, instanceOf(Future.class));
    }
}
