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
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Future;

import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonParseException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.IndexFunctionsDao;
import org.finra.herd.dao.TagDao;
import org.finra.herd.dao.helper.JsonHelper;
import org.finra.herd.model.api.xml.TagKey;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.SearchIndexUpdateDto;
import org.finra.herd.model.jpa.TagEntity;
import org.finra.herd.model.jpa.TagTypeEntity;
import org.finra.herd.service.helper.TagHelper;
import org.finra.herd.service.impl.TagServiceImpl;

/**
 * TagServiceIndexTest
 */
public class TagServiceIndexTest extends AbstractServiceTest
{
    @InjectMocks
    private TagServiceImpl tagService;

    @Mock
    private ConfigurationHelper configurationHelper;

    @Mock
    private IndexFunctionsDao indexFunctionsDao;

    @Mock
    private TagDao tagDao;

    @Mock
    private TagHelper tagHelper;

    @Mock
    private JsonHelper jsonHelper;

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

        List<Long> tagIds = new ArrayList<>();
        tagEntityList.forEach(tagEntity -> tagIds.add(tagEntity.getId()));

        // Create a document on the search index
        SearchIndexUpdateDto searchIndexUpdateDto = new SearchIndexUpdateDto(MESSAGE_TYPE_TAG_UPDATE, tagIds, SEARCH_INDEX_UPDATE_TYPE_CREATE);

        // Mock the call to external methods
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class)).thenReturn("DOCUMENT_TYPE");
        when(tagDao.getTagsByIds(tagIds)).thenReturn(tagEntityList);
        when(tagHelper.safeObjectMapperWriteValueAsString(any(TagEntity.class))).thenReturn("JSON_STRING");

        // Call the method under test
        tagService.updateSearchIndexDocumentTag(searchIndexUpdateDto);

        // Verify the calls to external methods
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);
        verify(tagDao, times(1)).getTagsByIds(tagIds);
        verify(tagHelper, times(2)).safeObjectMapperWriteValueAsString(any(TagEntity.class));
    }

    @Test
    public void testUpdateSearchIndexDocumentTagCreateEmpty() throws Exception
    {
        List<TagEntity> tagEntityList = new ArrayList<>();
        TagEntity tagEntity1 = tagDaoTestHelper.createTagEntity(new TagKey(TAG_TYPE, TAG_CODE), TAG_DISPLAY_NAME, TAG_DESCRIPTION);
        TagEntity tagEntity2 = tagDaoTestHelper.createTagEntity(new TagKey(TAG_TYPE_2, TAG_CODE_2), TAG_DISPLAY_NAME_2, TAG_DESCRIPTION_2);
        tagEntityList.add(tagEntity1);
        tagEntityList.add(tagEntity2);

        List<Long> tagIds = new ArrayList<>();
        tagEntityList.forEach(tagEntity -> tagIds.add(tagEntity.getId()));

        // Create a document on the search index
        SearchIndexUpdateDto searchIndexUpdateDto = new SearchIndexUpdateDto(MESSAGE_TYPE_TAG_UPDATE, tagIds, SEARCH_INDEX_UPDATE_TYPE_CREATE);

        // Mock the call to external methods
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class)).thenReturn("DOCUMENT_TYPE");
        when(tagDao.getTagsByIds(tagIds)).thenReturn(tagEntityList);
        when(tagHelper.safeObjectMapperWriteValueAsString(any(TagEntity.class))).thenReturn("");
        doNothing().when(indexFunctionsDao).createIndexDocuments(any(), any(), any());

        // Call the method under test
        tagService.updateSearchIndexDocumentTag(searchIndexUpdateDto);

        // Verify the calls to external methods
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);
        verify(tagDao, times(1)).getTagsByIds(tagIds);
        verify(tagHelper, times(2)).safeObjectMapperWriteValueAsString(any(TagEntity.class));
        verify(indexFunctionsDao, times(1)).createIndexDocuments(any(), any(), any());
    }

    @Test
    public void testUpdateSearchIndexDocumentTagUpdate() throws Exception
    {
        List<TagEntity> tagEntityList = new ArrayList<>();
        TagEntity tagEntity1 = tagDaoTestHelper.createTagEntity(new TagKey(TAG_TYPE, TAG_CODE), TAG_DISPLAY_NAME, TAG_DESCRIPTION);
        TagEntity tagEntity2 = tagDaoTestHelper.createTagEntity(new TagKey(TAG_TYPE_2, TAG_CODE_2), TAG_DISPLAY_NAME_2, TAG_DESCRIPTION_2);
        tagEntityList.add(tagEntity1);
        tagEntityList.add(tagEntity2);

        List<Long> tagIds = new ArrayList<>();
        tagEntityList.forEach(tagEntity -> tagIds.add(tagEntity.getId()));

        // Create a document on the search index
        SearchIndexUpdateDto searchIndexUpdateDto = new SearchIndexUpdateDto(MESSAGE_TYPE_TAG_UPDATE, tagIds, SEARCH_INDEX_UPDATE_TYPE_UPDATE);

        // Mock the call to external methods
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class)).thenReturn("DOCUMENT_TYPE");
        when(tagDao.getTagsByIds(tagIds)).thenReturn(tagEntityList);
        when(tagHelper.safeObjectMapperWriteValueAsString(any(TagEntity.class))).thenReturn("JSON_STRING");

        // Call the method under test
        tagService.updateSearchIndexDocumentTag(searchIndexUpdateDto);

        // Verify the calls to external methods
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);
        verify(tagDao, times(1)).getTagsByIds(tagIds);
        verify(tagHelper, times(2)).safeObjectMapperWriteValueAsString(any(TagEntity.class));
        verify(indexFunctionsDao, times(1)).updateIndexDocuments(any(), any(), any());
    }

    @Test
    public void testUpdateSearchIndexDocumentTagUpdateEmpty() throws Exception
    {
        List<TagEntity> tagEntityList = new ArrayList<>();
        TagEntity tagEntity1 = tagDaoTestHelper.createTagEntity(new TagKey(TAG_TYPE, TAG_CODE), TAG_DISPLAY_NAME, TAG_DESCRIPTION);
        TagEntity tagEntity2 = tagDaoTestHelper.createTagEntity(new TagKey(TAG_TYPE_2, TAG_CODE_2), TAG_DISPLAY_NAME_2, TAG_DESCRIPTION_2);
        tagEntityList.add(tagEntity1);
        tagEntityList.add(tagEntity2);

        List<Long> tagIds = new ArrayList<>();
        tagEntityList.forEach(tagEntity -> tagIds.add(tagEntity.getId()));

        // Create a document on the search index
        SearchIndexUpdateDto searchIndexUpdateDto = new SearchIndexUpdateDto(MESSAGE_TYPE_TAG_UPDATE, tagIds, SEARCH_INDEX_UPDATE_TYPE_UPDATE);

        // Mock the call to external methods
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class)).thenReturn("DOCUMENT_TYPE");
        when(tagDao.getTagsByIds(tagIds)).thenReturn(tagEntityList);
        when(tagHelper.safeObjectMapperWriteValueAsString(any(TagEntity.class))).thenReturn("");

        // Call the method under test
        tagService.updateSearchIndexDocumentTag(searchIndexUpdateDto);

        // Verify the calls to external methods
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);
        verify(tagDao, times(1)).getTagsByIds(tagIds);
        verify(tagHelper, times(2)).safeObjectMapperWriteValueAsString(any(TagEntity.class));
        verify(indexFunctionsDao, times(1)).updateIndexDocuments(any(), any(), any());
    }

    @Test
    public void testUpdateSearchIndexDocumentTagDelete() throws Exception
    {
        List<TagEntity> tagEntityList = new ArrayList<>();
        TagEntity tagEntity1 = tagDaoTestHelper.createTagEntity(new TagKey(TAG_TYPE, TAG_CODE), TAG_DISPLAY_NAME, TAG_DESCRIPTION);
        TagEntity tagEntity2 = tagDaoTestHelper.createTagEntity(new TagKey(TAG_TYPE_2, TAG_CODE_2), TAG_DISPLAY_NAME_2, TAG_DESCRIPTION_2);
        tagEntityList.add(tagEntity1);
        tagEntityList.add(tagEntity2);

        List<Long> tagIds = new ArrayList<>();
        tagEntityList.forEach(tagEntity -> tagIds.add(tagEntity.getId()));

        // Delete from the search index
        SearchIndexUpdateDto searchIndexUpdateDto = new SearchIndexUpdateDto(MESSAGE_TYPE_TAG_UPDATE, tagIds, SEARCH_INDEX_UPDATE_TYPE_DELETE);

        // Mock the call to external methods
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class)).thenReturn("DOCUMENT_TYPE");
        doNothing().when(indexFunctionsDao).deleteIndexDocuments(any(), any(), any());

        // Call the method under test
        tagService.updateSearchIndexDocumentTag(searchIndexUpdateDto);

        // Verify the calls to external methods
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);
        verify(indexFunctionsDao, times(1)).deleteIndexDocuments(any(), any(), any());
    }

    @Test
    public void testUpdateSearchIndexDocumentTagUnknown() throws Exception
    {
        List<TagEntity> tagEntityList = new ArrayList<>();
        TagEntity tagEntity1 = tagDaoTestHelper.createTagEntity(new TagKey(TAG_TYPE, TAG_CODE), TAG_DISPLAY_NAME, TAG_DESCRIPTION);
        TagEntity tagEntity2 = tagDaoTestHelper.createTagEntity(new TagKey(TAG_TYPE_2, TAG_CODE_2), TAG_DISPLAY_NAME_2, TAG_DESCRIPTION_2);
        tagEntityList.add(tagEntity1);
        tagEntityList.add(tagEntity2);

        List<Long> tagIds = new ArrayList<>();
        tagEntityList.forEach(tagEntity -> tagIds.add(tagEntity.getId()));

        // Unknown modification type
        SearchIndexUpdateDto searchIndexUpdateDto = new SearchIndexUpdateDto(MESSAGE_TYPE_TAG_UPDATE, tagIds, "UNKNOWN");

        // Mock the call to external methods
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class)).thenReturn("DOCUMENT_TYPE");

        // Call the method under test
        tagService.updateSearchIndexDocumentTag(searchIndexUpdateDto);

        // Verify the calls to external methods
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);
    }

    @Test
    public void testIndexValidateTags() throws Exception
    {
        // Create a tag type entity.
        TagTypeEntity tagTypeEntity = tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, TAG_TYPE_ORDER, TAG_TYPE_DESCRIPTION);

        // Create two root tag entities for the tag type with tag display name in reverse order.
        List<TagEntity> tagEntityList = Arrays.asList(tagDaoTestHelper.createTagEntity(tagTypeEntity, TAG_CODE, TAG_DISPLAY_NAME_2, TAG_DESCRIPTION),
            tagDaoTestHelper.createTagEntity(tagTypeEntity, TAG_CODE_2, TAG_DISPLAY_NAME, TAG_DESCRIPTION_2));

        List<String> tagEntityIdList = new ArrayList<>();
        tagEntityIdList.add("100");
        tagEntityIdList.add("101");
        tagEntityIdList.add("110");

        // Mock the call to external methods
        when(tagDao.getTags()).thenReturn(tagEntityList);
        when(tagHelper.safeObjectMapperWriteValueAsString(any(TagEntity.class))).thenReturn("JSON_STRING");
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class)).thenReturn("DOCUMENT_TYPE");

        // Call the method under test
        Future<Void> future = tagService.indexValidateAllTags(SEARCH_INDEX_TYPE_TAG);

        assertThat("Tag service index all tags method returned null value.", future, not(nullValue()));
        assertThat("Tag service index all tags method return value is not instance of future.", future, instanceOf(Future.class));

        // Verify the calls to external methods
        verify(tagDao).getTags();
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);
        verify(tagHelper).executeFunctionForTagEntities(eq("TAG"), eq("DOCUMENT_TYPE"), eq(tagEntityList), any());
    }

    @Test
    public void testIndexSizeCheckValidationTags() throws Exception
    {
        // Mock the call to external methods
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class)).thenReturn("DOCUMENT_TYPE");
        when(indexFunctionsDao.getNumberOfTypesInIndex(any(), any())).thenReturn(100L);
        when(tagDao.getCountOfAllTags()).thenReturn(100L);

        // Call the method under test
        boolean isIndexSizeValid = tagService.indexSizeCheckValidationTags(SEARCH_INDEX_TYPE_TAG);

        assertThat("Tag service index size validation is false when it should have been true.", isIndexSizeValid, is(true));

        // Verify the calls to external methods
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);
        verify(indexFunctionsDao).getNumberOfTypesInIndex(any(), any());
        verify(tagDao).getCountOfAllTags();
        verifyNoMoreInteractions(tagDao, indexFunctionsDao, configurationHelper);
    }

    @Test
    public void testIndexSizeCheckValidationTagsFalse() throws Exception
    {
        // Mock the call to external methods
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class)).thenReturn("DOCUMENT_TYPE");
        when(indexFunctionsDao.getNumberOfTypesInIndex(any(), any())).thenReturn(100L);
        when(tagDao.getCountOfAllTags()).thenReturn(200L);

        // Call the method under test
        boolean isIndexSizeValid = tagService.indexSizeCheckValidationTags(SEARCH_INDEX_TYPE_TAG);

        assertThat("Tag service index size validation is true when it should have been false.", isIndexSizeValid, is(false));

        // Verify the calls to external methods
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);
        verify(indexFunctionsDao).getNumberOfTypesInIndex(any(), any());
        verify(tagDao).getCountOfAllTags();
        verifyNoMoreInteractions(tagDao, configurationHelper);
    }

    @Test
    public void testIndexSpotCheckPercentageValidationTags() throws Exception
    {
        // Create a tag type entity.
        TagTypeEntity tagTypeEntity = tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, TAG_TYPE_ORDER, TAG_TYPE_DESCRIPTION);

        // Create two root tag entities for the tag type with tag display name in reverse order.
        List<TagEntity> rootTagEntities = Arrays.asList(tagDaoTestHelper.createTagEntity(tagTypeEntity, TAG_CODE, TAG_DISPLAY_NAME_2, TAG_DESCRIPTION),
            tagDaoTestHelper.createTagEntity(tagTypeEntity, TAG_CODE_2, TAG_DISPLAY_NAME, TAG_DESCRIPTION_2));

        // Mock the call to external methods
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_TAG_SPOT_CHECK_PERCENTAGE, Double.class)).thenReturn(0.2);
        when(tagDao.getPercentageOfAllTags(0.2)).thenReturn(rootTagEntities);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class)).thenReturn("DOCUMENT_TYPE");
        when(tagHelper.safeObjectMapperWriteValueAsString(any(TagEntity.class))).thenReturn("JSON_STRING");
        when(indexFunctionsDao.isValidDocumentIndex(any(),any(), any(), any())).thenReturn(true);

        // Call the method under test
        boolean isSpotCheckPercentageValid = tagService.indexSpotCheckPercentageValidationTags(SEARCH_INDEX_TYPE_TAG);

        assertThat("Tag service index spot check random validation is false when it should have been true.", isSpotCheckPercentageValid, is(true));

        // Verify the calls to external methods
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_TAG_SPOT_CHECK_PERCENTAGE, Double.class);
        verify(tagDao).getPercentageOfAllTags(0.2);
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);
        verify(tagHelper, times(2)).safeObjectMapperWriteValueAsString(any(TagEntity.class));
        verify(indexFunctionsDao, times(2)).isValidDocumentIndex(any(),any(), any(), any());
        verifyNoMoreInteractions(tagDao, indexFunctionsDao, configurationHelper, jsonHelper);
    }

    @Test
    public void testIndexSpotCheckPercentageValidationTagsFalse() throws Exception
    {
        // Create a tag type entity.
        TagTypeEntity tagTypeEntity = tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, TAG_TYPE_ORDER, TAG_TYPE_DESCRIPTION);

        // Create two root tag entities for the tag type with tag display name in reverse order.
        List<TagEntity> rootTagEntities = Arrays.asList(tagDaoTestHelper.createTagEntity(tagTypeEntity, TAG_CODE, TAG_DISPLAY_NAME_2, TAG_DESCRIPTION),
            tagDaoTestHelper.createTagEntity(tagTypeEntity, TAG_CODE_2, TAG_DISPLAY_NAME, TAG_DESCRIPTION_2));


        // Mock the call to external methods
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_TAG_SPOT_CHECK_PERCENTAGE, Double.class)).thenReturn(0.2);
        when(tagDao.getPercentageOfAllTags(0.2)).thenReturn(rootTagEntities);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class)).thenReturn("DOCUMENT_TYPE");
        when(tagHelper.safeObjectMapperWriteValueAsString(any(TagEntity.class))).thenReturn("JSON_STRING");
        when(indexFunctionsDao.isValidDocumentIndex(any(), any(), any(), any())).thenReturn(false);

        // Call the method under test
        boolean isSpotCheckPercentageValid = tagService.indexSpotCheckPercentageValidationTags(SEARCH_INDEX_TYPE_TAG);

        assertThat("Tag service index spot check random validation is true when it should have been false.", isSpotCheckPercentageValid, is(false));

        // Verify the calls to external methods
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_TAG_SPOT_CHECK_PERCENTAGE, Double.class);
        verify(tagDao).getPercentageOfAllTags(0.2);
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);
        verify(tagHelper, times(2)).safeObjectMapperWriteValueAsString(any(TagEntity.class));
        verify(indexFunctionsDao, times(2)).isValidDocumentIndex(any(), any(), any(), any());
        verifyNoMoreInteractions(tagDao, indexFunctionsDao, configurationHelper, jsonHelper);
    }

    @Test
    public void testIndexSpotCheckPercentageValidationTagsObjectMappingException() throws Exception
    {
        // Create a tag type entity.
        TagTypeEntity tagTypeEntity = tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, TAG_TYPE_ORDER, TAG_TYPE_DESCRIPTION);

        // Create two root tag entities for the tag type with tag display name in reverse order.
        List<TagEntity> rootTagEntities = Arrays.asList(tagDaoTestHelper.createTagEntity(tagTypeEntity, TAG_CODE, TAG_DISPLAY_NAME_2, TAG_DESCRIPTION),
            tagDaoTestHelper.createTagEntity(tagTypeEntity, TAG_CODE_2, TAG_DISPLAY_NAME, TAG_DESCRIPTION_2));

        // Mock the call to external methods
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_TAG_SPOT_CHECK_PERCENTAGE, Double.class)).thenReturn(0.2);
        when(tagDao.getPercentageOfAllTags(0.2)).thenReturn(rootTagEntities);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class)).thenReturn("DOCUMENT_TYPE");
        when(jsonHelper.objectToJson(any()))
            .thenThrow(new IllegalStateException(new JsonParseException("Failed to Parse", new JsonLocation("SRC", 100L, 1, 2))));

        // Call the method under test
        boolean isSpotCheckPercentageValid = tagService.indexSpotCheckPercentageValidationTags(SEARCH_INDEX_TYPE_TAG);

        assertThat("Tag service index spot check random validation is true when it should have been false.", isSpotCheckPercentageValid, is(false));

        // Verify the calls to external methods
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_TAG_SPOT_CHECK_PERCENTAGE, Double.class);
        verify(tagDao).getPercentageOfAllTags(0.2);
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);
        verify(tagHelper, times(2)).safeObjectMapperWriteValueAsString(any(TagEntity.class));
        verifyNoMoreInteractions(tagDao, configurationHelper, jsonHelper);
    }

    @Test
    public void testIndexSpotCheckMostRecentValidationTags() throws Exception
    {
        // Create a tag type entity.
        TagTypeEntity tagTypeEntity = tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, TAG_TYPE_ORDER, TAG_TYPE_DESCRIPTION);

        // Create two root tag entities for the tag type with tag display name in reverse order.
        List<TagEntity> rootTagEntities = Arrays.asList(tagDaoTestHelper.createTagEntity(tagTypeEntity, TAG_CODE, TAG_DISPLAY_NAME_2, TAG_DESCRIPTION),
            tagDaoTestHelper.createTagEntity(tagTypeEntity, TAG_CODE_2, TAG_DISPLAY_NAME, TAG_DESCRIPTION_2));

        // Mock the call to external methods
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_TAG_SPOT_CHECK_MOST_RECENT_NUMBER, Integer.class)).thenReturn(10);
        when(tagDao.getMostRecentTags(10)).thenReturn(rootTagEntities);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class)).thenReturn("DOCUMENT_TYPE");
        when(tagHelper.safeObjectMapperWriteValueAsString(any(TagEntity.class))).thenReturn("JSON_STRING");
        when(indexFunctionsDao.isValidDocumentIndex(any(), any(), any(), any())).thenReturn(true);

        // Call the method under test
        boolean isSpotCheckMostRecentValid = tagService.indexSpotCheckMostRecentValidationTags(SEARCH_INDEX_TYPE_TAG);

        assertThat("Tag service index spot check most recent validation is false when it should have been true.", isSpotCheckMostRecentValid, is(true));

        // Verify the calls to external methods
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_TAG_SPOT_CHECK_MOST_RECENT_NUMBER, Integer.class);
        verify(tagDao).getMostRecentTags(10);
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);
        verify(tagHelper, times(2)).safeObjectMapperWriteValueAsString(any(TagEntity.class));
        verify(indexFunctionsDao, times(2)).isValidDocumentIndex(any(), any(), any(), any());
        verifyNoMoreInteractions(tagDao, indexFunctionsDao, configurationHelper, jsonHelper, tagHelper);
    }

    @Test
    public void testIndexSpotCheckMostRecentValidationTagsFalse() throws Exception
    {
        // Create a tag type entity.
        TagTypeEntity tagTypeEntity = tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, TAG_TYPE_ORDER, TAG_TYPE_DESCRIPTION);

        // Create two root tag entities for the tag type with tag display name in reverse order.
        List<TagEntity> rootTagEntities = Arrays.asList(tagDaoTestHelper.createTagEntity(tagTypeEntity, TAG_CODE, TAG_DISPLAY_NAME_2, TAG_DESCRIPTION),
            tagDaoTestHelper.createTagEntity(tagTypeEntity, TAG_CODE_2, TAG_DISPLAY_NAME, TAG_DESCRIPTION_2));

        // Mock the call to external methods
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_TAG_SPOT_CHECK_MOST_RECENT_NUMBER, Integer.class)).thenReturn(10);
        when(tagDao.getMostRecentTags(10)).thenReturn(rootTagEntities);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class)).thenReturn("DOCUMENT_TYPE");
        when(tagHelper.safeObjectMapperWriteValueAsString(any(TagEntity.class))).thenReturn("JSON_STRING");
        // Call the method under test
        boolean isSpotCheckMostRecentValid = tagService.indexSpotCheckMostRecentValidationTags(SEARCH_INDEX_TYPE_TAG);

        assertThat("Tag service index spot check most recent validation is true when it should have been false.", isSpotCheckMostRecentValid, is(false));

        // Verify the calls to external methods
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_TAG_SPOT_CHECK_MOST_RECENT_NUMBER, Integer.class);
        verify(tagDao).getMostRecentTags(10);
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);
        verify(tagHelper, times(2)).safeObjectMapperWriteValueAsString(any(TagEntity.class));
        verifyNoMoreInteractions(tagDao, configurationHelper, jsonHelper, tagHelper);
    }

    @Test
    public void testIndexSpotCheckMostRecentValidationTagsObjectMappingException() throws Exception
    {
        // Create a tag type entity.
        TagTypeEntity tagTypeEntity = tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, TAG_TYPE_ORDER, TAG_TYPE_DESCRIPTION);

        // Create two root tag entities for the tag type with tag display name in reverse order.
        List<TagEntity> rootTagEntities = Arrays.asList(tagDaoTestHelper.createTagEntity(tagTypeEntity, TAG_CODE, TAG_DISPLAY_NAME_2, TAG_DESCRIPTION),
            tagDaoTestHelper.createTagEntity(tagTypeEntity, TAG_CODE_2, TAG_DISPLAY_NAME, TAG_DESCRIPTION_2));

        // Mock the call to external methods
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_TAG_SPOT_CHECK_MOST_RECENT_NUMBER, Integer.class)).thenReturn(10);
        when(tagDao.getMostRecentTags(10)).thenReturn(rootTagEntities);
        when(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class)).thenReturn("DOCUMENT_TYPE");
        when(jsonHelper.objectToJson(any()))
            .thenThrow(new IllegalStateException(new JsonParseException("Failed to Parse", new JsonLocation("SRC", 100L, 1, 2))));
        when(indexFunctionsDao.isValidDocumentIndex(any(), any(), any(), any())).thenReturn(false);

        // Call the method under test
        boolean isSpotCheckMostRecentValid = tagService.indexSpotCheckMostRecentValidationTags(SEARCH_INDEX_TYPE_TAG);

        assertThat("Tag service index spot check most recent validation is true when it should have been false.", isSpotCheckMostRecentValid, is(false));

        // Verify the calls to external methods
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_TAG_SPOT_CHECK_MOST_RECENT_NUMBER, Integer.class);
        verify(tagDao).getMostRecentTags(10);
        verify(configurationHelper).getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);
        verify(tagHelper, times(2)).safeObjectMapperWriteValueAsString(any(TagEntity.class));
        verify(indexFunctionsDao, times(2)).isValidDocumentIndex(any(), any(), any(), any());
        verifyNoMoreInteractions(tagDao, indexFunctionsDao, configurationHelper, jsonHelper, tagHelper);
    }
}
