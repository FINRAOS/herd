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

import static org.finra.herd.model.dto.SearchIndexUpdateDto.MESSAGE_TYPE_TAG_UPDATE;
import static org.finra.herd.model.dto.SearchIndexUpdateDto.SEARCH_INDEX_UPDATE_TYPE_CREATE;
import static org.finra.herd.model.dto.SearchIndexUpdateDto.SEARCH_INDEX_UPDATE_TYPE_DELETE;
import static org.finra.herd.model.dto.SearchIndexUpdateDto.SEARCH_INDEX_UPDATE_TYPE_UPDATE;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.TagDao;
import org.finra.herd.model.api.xml.TagKey;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.SearchIndexUpdateDto;
import org.finra.herd.model.jpa.TagEntity;
import org.finra.herd.service.functional.SearchFunctions;
import org.finra.herd.service.helper.TagDaoHelper;
import org.finra.herd.service.impl.TagServiceImpl;

/**
 * TagServiceIndexTest
 */
public class TagServiceIndexTest  extends AbstractServiceTest
{
    @InjectMocks
    private TagServiceImpl tagService;

    @Mock
    private ConfigurationHelper configurationHelper;

    @Mock
    private SearchFunctions searchFunctions;

    @Mock
    private TagDao tagDao;

    @Mock
    private TagDaoHelper tagDaoHelper;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testUpdateSearchIndexDocumentTagCreate() throws Exception
    {
        List<TagEntity> tagEntityList = new ArrayList<>();
        TagEntity tagEntity1 = tagDaoTestHelper.createTagEntity(new TagKey(TAG_TYPE, TAG_CODE), TAG_DISPLAY_NAME, TAG_DESCRIPTION);
        TagEntity tagEntity2 = tagDaoTestHelper.createTagEntity(new TagKey(TAG_TYPE_2, TAG_CODE_2), TAG_DISPLAY_NAME_2, TAG_DESCRIPTION_2);
        tagEntityList.add(tagEntity1);
        tagEntityList.add(tagEntity2);

        List<Integer> tagIds = new ArrayList<>();
        tagEntityList.forEach(tagEntity -> tagIds.add(tagEntity.getId()));

        // Create a document on the search index
        SearchIndexUpdateDto searchIndexUpdateDto =
            new SearchIndexUpdateDto(MESSAGE_TYPE_TAG_UPDATE, tagIds, SEARCH_INDEX_UPDATE_TYPE_CREATE);

        // Mock the call to external methods
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class)).thenReturn("DOCUMENT_TYPE");
        when(tagDao.getTagsByIds(tagIds)).thenReturn(tagEntityList);
        when(tagDaoHelper.safeObjectMapperWriteValueAsString(any(TagEntity.class))).thenReturn("JSON_STRING");
        when(searchFunctions.getCreateIndexDocumentsFunction()).thenReturn((indexName, documentType, map) ->
        {
        });

        // Call the method under test
        tagService.updateSearchIndexDocumentTag(searchIndexUpdateDto);

        // Verify the calls to external methods
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);
        verify(tagDao, times(1)).getTagsByIds(tagIds);
        verify(tagDaoHelper, times(2)).safeObjectMapperWriteValueAsString(any(TagEntity.class));
        verify(searchFunctions, times(1)).getCreateIndexDocumentsFunction();
    }

    @Test
    public void testUpdateSearchIndexDocumentTagCreateEmpty() throws Exception
    {
        List<TagEntity> tagEntityList = new ArrayList<>();
        TagEntity tagEntity1 = tagDaoTestHelper.createTagEntity(new TagKey(TAG_TYPE, TAG_CODE), TAG_DISPLAY_NAME, TAG_DESCRIPTION);
        TagEntity tagEntity2 = tagDaoTestHelper.createTagEntity(new TagKey(TAG_TYPE_2, TAG_CODE_2), TAG_DISPLAY_NAME_2, TAG_DESCRIPTION_2);
        tagEntityList.add(tagEntity1);
        tagEntityList.add(tagEntity2);

        List<Integer> tagIds = new ArrayList<>();
        tagEntityList.forEach(tagEntity -> tagIds.add(tagEntity.getId()));

        // Create a document on the search index
        SearchIndexUpdateDto searchIndexUpdateDto =
            new SearchIndexUpdateDto(MESSAGE_TYPE_TAG_UPDATE, tagIds, SEARCH_INDEX_UPDATE_TYPE_CREATE);

        // Mock the call to external methods
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class)).thenReturn("DOCUMENT_TYPE");
        when(tagDao.getTagsByIds(tagIds)).thenReturn(tagEntityList);
        when(tagDaoHelper.safeObjectMapperWriteValueAsString(any(TagEntity.class))).thenReturn("");
        when(searchFunctions.getCreateIndexDocumentsFunction()).thenReturn((indexName, documentType, map) ->
        {
        });

        // Call the method under test
        tagService.updateSearchIndexDocumentTag(searchIndexUpdateDto);

        // Verify the calls to external methods
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);
        verify(tagDao, times(1)).getTagsByIds(tagIds);
        verify(tagDaoHelper, times(2)).safeObjectMapperWriteValueAsString(any(TagEntity.class));
        verify(searchFunctions, times(1)).getCreateIndexDocumentsFunction();
    }

    @Test
    public void testUpdateSearchIndexDocumentBusinessObjectDefinitionUpdate() throws Exception
    {
        List<TagEntity> tagEntityList = new ArrayList<>();
        TagEntity tagEntity1 = tagDaoTestHelper.createTagEntity(new TagKey(TAG_TYPE, TAG_CODE), TAG_DISPLAY_NAME, TAG_DESCRIPTION);
        TagEntity tagEntity2 = tagDaoTestHelper.createTagEntity(new TagKey(TAG_TYPE_2, TAG_CODE_2), TAG_DISPLAY_NAME_2, TAG_DESCRIPTION_2);
        tagEntityList.add(tagEntity1);
        tagEntityList.add(tagEntity2);

        List<Integer> tagIds = new ArrayList<>();
        tagEntityList.forEach(tagEntity -> tagIds.add(tagEntity.getId()));

        // Create a document on the search index
        SearchIndexUpdateDto searchIndexUpdateDto =
            new SearchIndexUpdateDto(MESSAGE_TYPE_TAG_UPDATE, tagIds, SEARCH_INDEX_UPDATE_TYPE_UPDATE);

        // Mock the call to external methods
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class)).thenReturn("DOCUMENT_TYPE");
        when(tagDao.getTagsByIds(tagIds)).thenReturn(tagEntityList);
        when(tagDaoHelper.safeObjectMapperWriteValueAsString(any(TagEntity.class))).thenReturn("JSON_STRING");
        when(searchFunctions.getUpdateIndexDocumentsFunction()).thenReturn((indexName, documentType, map) ->
        {
        });

        // Call the method under test
        tagService.updateSearchIndexDocumentTag(searchIndexUpdateDto);

        // Verify the calls to external methods
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);
        verify(tagDao, times(1)).getTagsByIds(tagIds);
        verify(tagDaoHelper, times(2)).safeObjectMapperWriteValueAsString(any(TagEntity.class));
        verify(searchFunctions, times(1)).getUpdateIndexDocumentsFunction();
    }

    @Test
    public void testUpdateSearchIndexDocumentBusinessObjectDefinitionUpdateEmpty() throws Exception
    {
        List<TagEntity> tagEntityList = new ArrayList<>();
        TagEntity tagEntity1 = tagDaoTestHelper.createTagEntity(new TagKey(TAG_TYPE, TAG_CODE), TAG_DISPLAY_NAME, TAG_DESCRIPTION);
        TagEntity tagEntity2 = tagDaoTestHelper.createTagEntity(new TagKey(TAG_TYPE_2, TAG_CODE_2), TAG_DISPLAY_NAME_2, TAG_DESCRIPTION_2);
        tagEntityList.add(tagEntity1);
        tagEntityList.add(tagEntity2);

        List<Integer> tagIds = new ArrayList<>();
        tagEntityList.forEach(tagEntity -> tagIds.add(tagEntity.getId()));

        // Create a document on the search index
        SearchIndexUpdateDto searchIndexUpdateDto =
            new SearchIndexUpdateDto(MESSAGE_TYPE_TAG_UPDATE, tagIds, SEARCH_INDEX_UPDATE_TYPE_UPDATE);

        // Mock the call to external methods
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class)).thenReturn("DOCUMENT_TYPE");
        when(tagDao.getTagsByIds(tagIds)).thenReturn(tagEntityList);
        when(tagDaoHelper.safeObjectMapperWriteValueAsString(any(TagEntity.class))).thenReturn("");
        when(searchFunctions.getUpdateIndexDocumentsFunction()).thenReturn((indexName, documentType, map) ->
        {
        });

        // Call the method under test
        tagService.updateSearchIndexDocumentTag(searchIndexUpdateDto);

        // Verify the calls to external methods
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);
        verify(tagDao, times(1)).getTagsByIds(tagIds);
        verify(tagDaoHelper, times(2)).safeObjectMapperWriteValueAsString(any(TagEntity.class));
        verify(searchFunctions, times(1)).getUpdateIndexDocumentsFunction();
    }

    @Test
    public void testUpdateSearchIndexDocumentBusinessObjectDefinitionDelete() throws Exception
    {
        List<TagEntity> tagEntityList = new ArrayList<>();
        TagEntity tagEntity1 = tagDaoTestHelper.createTagEntity(new TagKey(TAG_TYPE, TAG_CODE), TAG_DISPLAY_NAME, TAG_DESCRIPTION);
        TagEntity tagEntity2 = tagDaoTestHelper.createTagEntity(new TagKey(TAG_TYPE_2, TAG_CODE_2), TAG_DISPLAY_NAME_2, TAG_DESCRIPTION_2);
        tagEntityList.add(tagEntity1);
        tagEntityList.add(tagEntity2);

        List<Integer> tagIds = new ArrayList<>();
        tagEntityList.forEach(tagEntity -> tagIds.add(tagEntity.getId()));

        // Delete from the search index
        SearchIndexUpdateDto searchIndexUpdateDto =
            new SearchIndexUpdateDto(MESSAGE_TYPE_TAG_UPDATE, tagIds, SEARCH_INDEX_UPDATE_TYPE_DELETE);

        // Mock the call to external methods
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class)).thenReturn("DOCUMENT_TYPE");
        when(searchFunctions.getDeleteIndexDocumentsFunction()).thenReturn((indexName, documentType, map) ->
        {
        });

        // Call the method under test
        tagService.updateSearchIndexDocumentTag(searchIndexUpdateDto);

        // Verify the calls to external methods
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);
        verify(searchFunctions, times(1)).getDeleteIndexDocumentsFunction();
    }

    @Test
    public void testUpdateSearchIndexDocumentBusinessObjectDefinitionUnknown() throws Exception
    {
        List<TagEntity> tagEntityList = new ArrayList<>();
        TagEntity tagEntity1 = tagDaoTestHelper.createTagEntity(new TagKey(TAG_TYPE, TAG_CODE), TAG_DISPLAY_NAME, TAG_DESCRIPTION);
        TagEntity tagEntity2 = tagDaoTestHelper.createTagEntity(new TagKey(TAG_TYPE_2, TAG_CODE_2), TAG_DISPLAY_NAME_2, TAG_DESCRIPTION_2);
        tagEntityList.add(tagEntity1);
        tagEntityList.add(tagEntity2);

        List<Integer> tagIds = new ArrayList<>();
        tagEntityList.forEach(tagEntity -> tagIds.add(tagEntity.getId()));

        // Unknown modification type
        SearchIndexUpdateDto searchIndexUpdateDto =
            new SearchIndexUpdateDto(MESSAGE_TYPE_TAG_UPDATE, tagIds, "UNKNOWN");

        // Mock the call to external methods
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class)).thenReturn("DOCUMENT_TYPE");

        // Call the method under test
        tagService.updateSearchIndexDocumentTag(searchIndexUpdateDto);

        // Verify the calls to external methods
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);
    }

}
