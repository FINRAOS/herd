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

import static org.finra.herd.model.dto.SearchIndexUpdateDto.MESSAGE_TYPE_BUSINESS_OBJECT_DEFINITION_UPDATE;
import static org.finra.herd.model.dto.SearchIndexUpdateDto.MESSAGE_TYPE_TAG_UPDATE;
import static org.finra.herd.model.dto.SearchIndexUpdateDto.SEARCH_INDEX_UPDATE_TYPE_UPDATE;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;
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
import org.finra.herd.dao.helper.JsonHelper;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.SearchIndexUpdateDto;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.model.jpa.TagEntity;

/**
 * SearchIndexUpdateHelperTest
 */
public class SearchIndexUpdateHelperTest
{
    @InjectMocks
    private SearchIndexUpdateHelper searchIndexUpdateHelper;

    @Mock
    private ConfigurationHelper configurationHelper;

    @Mock
    private NotificationMessageInMemoryQueue jmsMessageInMemoryQueue;

    @Mock
    private JsonHelper jsonHelper;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testModifyBusinessObjectDefinitionInSearchIndex()
    {
        // Create a business object data entity
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = new BusinessObjectDefinitionEntity();
        businessObjectDefinitionEntity.setId(1);

        // Create a list of business object definition ids
        List<Integer> businessObjectDefinitionIds = new ArrayList<>();
        businessObjectDefinitionIds.add(businessObjectDefinitionEntity.getId());

        // Create a search index dto
        SearchIndexUpdateDto searchIndexUpdateDto =
            new SearchIndexUpdateDto(MESSAGE_TYPE_BUSINESS_OBJECT_DEFINITION_UPDATE, businessObjectDefinitionIds, SEARCH_INDEX_UPDATE_TYPE_UPDATE);

        // Mock the call to external methods
        when(jsonHelper.objectToJson(searchIndexUpdateDto)).thenReturn("MESSAGE TEXT");
        when(configurationHelper.getProperty(ConfigurationValue.SEARCH_INDEX_UPDATE_JMS_LISTENER_ENABLED)).thenReturn("true");
        when(configurationHelper.getProperty(ConfigurationValue.SEARCH_INDEX_UPDATE_SQS_QUEUE_NAME)).thenReturn("QUEUE NAME");

        // Call the method under test
        searchIndexUpdateHelper.modifyBusinessObjectDefinitionInSearchIndex(businessObjectDefinitionEntity, SEARCH_INDEX_UPDATE_TYPE_UPDATE);

        // Verify the calls to external methods
        verify(jsonHelper, times(1)).objectToJson(searchIndexUpdateDto);
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.SEARCH_INDEX_UPDATE_JMS_LISTENER_ENABLED);
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.SEARCH_INDEX_UPDATE_SQS_QUEUE_NAME);
    }

    @Test
    public void testModifyBusinessObjectDefinitionInSearchIndexEmptyQueueName()
    {
        // Create a business object data entity
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = new BusinessObjectDefinitionEntity();
        businessObjectDefinitionEntity.setId(1);

        // Create a list of business object definition ids
        List<Integer> businessObjectDefinitionIds = new ArrayList<>();
        businessObjectDefinitionIds.add(businessObjectDefinitionEntity.getId());

        // Create a search index dto
        SearchIndexUpdateDto searchIndexUpdateDto =
            new SearchIndexUpdateDto(MESSAGE_TYPE_BUSINESS_OBJECT_DEFINITION_UPDATE, businessObjectDefinitionIds, SEARCH_INDEX_UPDATE_TYPE_UPDATE);

        // Mock the call to external methods
        when(jsonHelper.objectToJson(searchIndexUpdateDto)).thenReturn("MESSAGE TEXT");
        when(configurationHelper.getProperty(ConfigurationValue.SEARCH_INDEX_UPDATE_JMS_LISTENER_ENABLED)).thenReturn("true");
        when(configurationHelper.getProperty(ConfigurationValue.SEARCH_INDEX_UPDATE_SQS_QUEUE_NAME)).thenReturn("");

        // Call the method under test
        try
        {
            searchIndexUpdateHelper.modifyBusinessObjectDefinitionInSearchIndex(businessObjectDefinitionEntity, SEARCH_INDEX_UPDATE_TYPE_UPDATE);
            fail("Should have thrown an IllegalStateException");
        }
        catch (IllegalStateException illegalStateException)
        {
            assertThat("Function is null.", illegalStateException.getMessage(),
                is("SQS queue name not found. Ensure the \"search.index.update.sqs.queue.name\" configuration entry is configured."));
        }

        // Verify the calls to external methods
        verify(jsonHelper, times(1)).objectToJson(searchIndexUpdateDto);
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.SEARCH_INDEX_UPDATE_JMS_LISTENER_ENABLED);
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.SEARCH_INDEX_UPDATE_SQS_QUEUE_NAME);
    }

    @Test
    public void testModifyBusinessObjectDefinitionInSearchIndexBlankMessage()
    {
        // Create a business object data entity
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = new BusinessObjectDefinitionEntity();
        businessObjectDefinitionEntity.setId(1);

        // Create a list of business object definition ids
        List<Integer> businessObjectDefinitionIds = new ArrayList<>();
        businessObjectDefinitionIds.add(businessObjectDefinitionEntity.getId());

        // Create a search index dto
        SearchIndexUpdateDto searchIndexUpdateDto =
            new SearchIndexUpdateDto(MESSAGE_TYPE_BUSINESS_OBJECT_DEFINITION_UPDATE, businessObjectDefinitionIds, SEARCH_INDEX_UPDATE_TYPE_UPDATE);

        // Mock the call to external methods
        when(jsonHelper.objectToJson(searchIndexUpdateDto)).thenReturn("");
        when(configurationHelper.getProperty(ConfigurationValue.SEARCH_INDEX_UPDATE_JMS_LISTENER_ENABLED)).thenReturn("true");
        when(configurationHelper.getProperty(ConfigurationValue.SEARCH_INDEX_UPDATE_SQS_QUEUE_NAME)).thenReturn("QUEUE NAME");

        // Call the method under test
        searchIndexUpdateHelper.modifyBusinessObjectDefinitionInSearchIndex(businessObjectDefinitionEntity, SEARCH_INDEX_UPDATE_TYPE_UPDATE);

        // Verify the calls to external methods
        verify(jsonHelper, times(1)).objectToJson(searchIndexUpdateDto);
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.SEARCH_INDEX_UPDATE_JMS_LISTENER_ENABLED);
    }

    @Test
    public void testModifyBusinessObjectDefinitionsInSearchIndex()
    {
        // Create a business object data entity
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = new BusinessObjectDefinitionEntity();
        businessObjectDefinitionEntity.setId(1);
        List<BusinessObjectDefinitionEntity> businessObjectDefinitionEntityList = new ArrayList<>();
        businessObjectDefinitionEntityList.add(businessObjectDefinitionEntity);

        // Create a list of business object definition ids
        List<Integer> businessObjectDefinitionIds = new ArrayList<>();
        businessObjectDefinitionIds.add(businessObjectDefinitionEntity.getId());

        // Create a search index dto
        SearchIndexUpdateDto searchIndexUpdateDto =
            new SearchIndexUpdateDto(MESSAGE_TYPE_BUSINESS_OBJECT_DEFINITION_UPDATE, businessObjectDefinitionIds, SEARCH_INDEX_UPDATE_TYPE_UPDATE);

        // Mock the call to external methods
        when(jsonHelper.objectToJson(searchIndexUpdateDto)).thenReturn("MESSAGE TEXT");
        when(configurationHelper.getProperty(ConfigurationValue.SEARCH_INDEX_UPDATE_JMS_LISTENER_ENABLED)).thenReturn("true");
        when(configurationHelper.getProperty(ConfigurationValue.SEARCH_INDEX_UPDATE_SQS_QUEUE_NAME)).thenReturn("QUEUE NAME");

        // Call the method under test
        searchIndexUpdateHelper.modifyBusinessObjectDefinitionsInSearchIndex(businessObjectDefinitionEntityList, SEARCH_INDEX_UPDATE_TYPE_UPDATE);

        // Verify the calls to external methods
        verify(jsonHelper, times(1)).objectToJson(searchIndexUpdateDto);
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.SEARCH_INDEX_UPDATE_JMS_LISTENER_ENABLED);
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.SEARCH_INDEX_UPDATE_SQS_QUEUE_NAME);
    }

    @Test
    public void testModifyBusinessObjectDefinitionsInSearchIndexEmptyQueueName()
    {
        // Create a business object data entity
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = new BusinessObjectDefinitionEntity();
        businessObjectDefinitionEntity.setId(1);
        List<BusinessObjectDefinitionEntity> businessObjectDefinitionEntityList = new ArrayList<>();
        businessObjectDefinitionEntityList.add(businessObjectDefinitionEntity);

        // Create a list of business object definition ids
        List<Integer> businessObjectDefinitionIds = new ArrayList<>();
        businessObjectDefinitionIds.add(businessObjectDefinitionEntity.getId());

        // Create a search index dto
        SearchIndexUpdateDto searchIndexUpdateDto =
            new SearchIndexUpdateDto(MESSAGE_TYPE_BUSINESS_OBJECT_DEFINITION_UPDATE, businessObjectDefinitionIds, SEARCH_INDEX_UPDATE_TYPE_UPDATE);

        // Mock the call to external methods
        when(jsonHelper.objectToJson(searchIndexUpdateDto)).thenReturn("MESSAGE TEXT");
        when(configurationHelper.getProperty(ConfigurationValue.SEARCH_INDEX_UPDATE_JMS_LISTENER_ENABLED)).thenReturn("true");
        when(configurationHelper.getProperty(ConfigurationValue.SEARCH_INDEX_UPDATE_SQS_QUEUE_NAME)).thenReturn("");

        // Call the method under test
        try
        {
            searchIndexUpdateHelper.modifyBusinessObjectDefinitionsInSearchIndex(businessObjectDefinitionEntityList, SEARCH_INDEX_UPDATE_TYPE_UPDATE);
            fail("Should have thrown an IllegalStateException");
        }
        catch (IllegalStateException illegalStateException)
        {
            assertThat("Function is null.", illegalStateException.getMessage(),
                is("SQS queue name not found. Ensure the \"search.index.update.sqs.queue.name\" configuration entry is configured."));
        }

        // Verify the calls to external methods
        verify(jsonHelper, times(1)).objectToJson(searchIndexUpdateDto);
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.SEARCH_INDEX_UPDATE_JMS_LISTENER_ENABLED);
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.SEARCH_INDEX_UPDATE_SQS_QUEUE_NAME);
    }

    @Test
    public void testModifyBusinessObjectDefinitionsInSearchIndexBlankMessage()
    {
        // Create a business object data entity
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = new BusinessObjectDefinitionEntity();
        businessObjectDefinitionEntity.setId(1);
        List<BusinessObjectDefinitionEntity> businessObjectDefinitionEntityList = new ArrayList<>();
        businessObjectDefinitionEntityList.add(businessObjectDefinitionEntity);

        // Create a list of business object definition ids
        List<Integer> businessObjectDefinitionIds = new ArrayList<>();
        businessObjectDefinitionIds.add(businessObjectDefinitionEntity.getId());

        // Create a search index dto
        SearchIndexUpdateDto searchIndexUpdateDto =
            new SearchIndexUpdateDto(MESSAGE_TYPE_BUSINESS_OBJECT_DEFINITION_UPDATE, businessObjectDefinitionIds, SEARCH_INDEX_UPDATE_TYPE_UPDATE);

        // Mock the call to external methods
        when(jsonHelper.objectToJson(searchIndexUpdateDto)).thenReturn("");
        when(configurationHelper.getProperty(ConfigurationValue.SEARCH_INDEX_UPDATE_JMS_LISTENER_ENABLED)).thenReturn("true");
        when(configurationHelper.getProperty(ConfigurationValue.SEARCH_INDEX_UPDATE_SQS_QUEUE_NAME)).thenReturn("QUEUE NAME");

        // Call the method under test
        searchIndexUpdateHelper.modifyBusinessObjectDefinitionsInSearchIndex(businessObjectDefinitionEntityList, SEARCH_INDEX_UPDATE_TYPE_UPDATE);

        // Verify the calls to external methods
        verify(jsonHelper, times(1)).objectToJson(searchIndexUpdateDto);
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.SEARCH_INDEX_UPDATE_JMS_LISTENER_ENABLED);
    }

    @Test
    public void testModifyTagInSearchIndex()
    {
        // Create a tag entity
        TagEntity tagEntity = new TagEntity();
        tagEntity.setId(1);

        // Create a list of tag ids
        List<Integer> tagIds = new ArrayList<>();
        tagIds.add(tagEntity.getId());

        // Create a search index dto
        SearchIndexUpdateDto searchIndexUpdateDto = new SearchIndexUpdateDto(MESSAGE_TYPE_TAG_UPDATE, tagIds, SEARCH_INDEX_UPDATE_TYPE_UPDATE);

        // Mock the call to external methods
        when(jsonHelper.objectToJson(searchIndexUpdateDto)).thenReturn("MESSAGE TEXT");
        when(configurationHelper.getProperty(ConfigurationValue.SEARCH_INDEX_UPDATE_JMS_LISTENER_ENABLED)).thenReturn("true");
        when(configurationHelper.getProperty(ConfigurationValue.SEARCH_INDEX_UPDATE_SQS_QUEUE_NAME)).thenReturn("QUEUE NAME");

        // Call the method under test
        searchIndexUpdateHelper.modifyTagInSearchIndex(tagEntity, SEARCH_INDEX_UPDATE_TYPE_UPDATE);

        // Verify the calls to external methods
        verify(jsonHelper, times(1)).objectToJson(searchIndexUpdateDto);
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.SEARCH_INDEX_UPDATE_JMS_LISTENER_ENABLED);
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.SEARCH_INDEX_UPDATE_SQS_QUEUE_NAME);
    }

    @Test
    public void testModifyTagInSearchIndexEmptyQueueName()
    {
        // Create a tag entity
        TagEntity tagEntity = new TagEntity();
        tagEntity.setId(1);

        // Create a list of tag ids
        List<Integer> tagIds = new ArrayList<>();
        tagIds.add(tagEntity.getId());

        // Create a search index dto
        SearchIndexUpdateDto searchIndexUpdateDto = new SearchIndexUpdateDto(MESSAGE_TYPE_TAG_UPDATE, tagIds, SEARCH_INDEX_UPDATE_TYPE_UPDATE);

        // Mock the call to external methods
        when(jsonHelper.objectToJson(searchIndexUpdateDto)).thenReturn("MESSAGE TEXT");
        when(configurationHelper.getProperty(ConfigurationValue.SEARCH_INDEX_UPDATE_JMS_LISTENER_ENABLED)).thenReturn("true");
        when(configurationHelper.getProperty(ConfigurationValue.SEARCH_INDEX_UPDATE_SQS_QUEUE_NAME)).thenReturn("");

        // Call the method under test
        try
        {
            searchIndexUpdateHelper.modifyTagInSearchIndex(tagEntity, SEARCH_INDEX_UPDATE_TYPE_UPDATE);
            fail("Should have thrown an IllegalStateException");
        }
        catch (IllegalStateException illegalStateException)
        {
            assertThat("Function is null.", illegalStateException.getMessage(),
                is("SQS queue name not found. Ensure the \"search.index.update.sqs.queue.name\" configuration entry is configured."));
        }

        // Verify the calls to external methods
        verify(jsonHelper, times(1)).objectToJson(searchIndexUpdateDto);
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.SEARCH_INDEX_UPDATE_JMS_LISTENER_ENABLED);
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.SEARCH_INDEX_UPDATE_SQS_QUEUE_NAME);
    }

    @Test
    public void testModifyTagInSearchIndexBlankMessage()
    {
        // Create a tag entity
        TagEntity tagEntity = new TagEntity();
        tagEntity.setId(1);

        // Create a list of tag ids
        List<Integer> tagIds = new ArrayList<>();
        tagIds.add(tagEntity.getId());

        // Create a search index dto
        SearchIndexUpdateDto searchIndexUpdateDto = new SearchIndexUpdateDto(MESSAGE_TYPE_TAG_UPDATE, tagIds, SEARCH_INDEX_UPDATE_TYPE_UPDATE);

        // Mock the call to external methods
        when(jsonHelper.objectToJson(searchIndexUpdateDto)).thenReturn("");
        when(configurationHelper.getProperty(ConfigurationValue.SEARCH_INDEX_UPDATE_JMS_LISTENER_ENABLED)).thenReturn("true");
        when(configurationHelper.getProperty(ConfigurationValue.SEARCH_INDEX_UPDATE_SQS_QUEUE_NAME)).thenReturn("QUEUE NAME");

        // Call the method under test
        searchIndexUpdateHelper.modifyTagInSearchIndex(tagEntity, SEARCH_INDEX_UPDATE_TYPE_UPDATE);

        // Verify the calls to external methods
        verify(jsonHelper, times(1)).objectToJson(searchIndexUpdateDto);
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.SEARCH_INDEX_UPDATE_JMS_LISTENER_ENABLED);
    }

    @Test
    public void testModifyTagsInSearchIndex()
    {
        // Create a tag entity list
        TagEntity tagEntity = new TagEntity();
        tagEntity.setId(1);
        List<TagEntity> tagEntityList = new ArrayList<>();
        tagEntityList.add(tagEntity);

        // Create a list of tag ids
        List<Integer> tagIds = new ArrayList<>();
        tagIds.add(tagEntity.getId());

        // Create a search index dto
        SearchIndexUpdateDto searchIndexUpdateDto = new SearchIndexUpdateDto(MESSAGE_TYPE_TAG_UPDATE, tagIds, SEARCH_INDEX_UPDATE_TYPE_UPDATE);

        // Mock the call to external methods
        when(jsonHelper.objectToJson(searchIndexUpdateDto)).thenReturn("MESSAGE TEXT");
        when(configurationHelper.getProperty(ConfigurationValue.SEARCH_INDEX_UPDATE_JMS_LISTENER_ENABLED)).thenReturn("true");
        when(configurationHelper.getProperty(ConfigurationValue.SEARCH_INDEX_UPDATE_SQS_QUEUE_NAME)).thenReturn("QUEUE NAME");

        // Call the method under test
        searchIndexUpdateHelper.modifyTagsInSearchIndex(tagEntityList, SEARCH_INDEX_UPDATE_TYPE_UPDATE);

        // Verify the calls to external methods
        verify(jsonHelper, times(1)).objectToJson(searchIndexUpdateDto);
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.SEARCH_INDEX_UPDATE_JMS_LISTENER_ENABLED);
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.SEARCH_INDEX_UPDATE_SQS_QUEUE_NAME);
    }

    @Test
    public void testModifyTagsInSearchIndexEmptyQueueName()
    {
        // Create a tag entity list
        TagEntity tagEntity = new TagEntity();
        tagEntity.setId(1);
        List<TagEntity> tagEntityList = new ArrayList<>();
        tagEntityList.add(tagEntity);

        // Create a list of tag ids
        List<Integer> tagIds = new ArrayList<>();
        tagIds.add(tagEntity.getId());

        // Create a search index dto
        SearchIndexUpdateDto searchIndexUpdateDto = new SearchIndexUpdateDto(MESSAGE_TYPE_TAG_UPDATE, tagIds, SEARCH_INDEX_UPDATE_TYPE_UPDATE);

        // Mock the call to external methods
        when(jsonHelper.objectToJson(searchIndexUpdateDto)).thenReturn("MESSAGE TEXT");
        when(configurationHelper.getProperty(ConfigurationValue.SEARCH_INDEX_UPDATE_JMS_LISTENER_ENABLED)).thenReturn("true");
        when(configurationHelper.getProperty(ConfigurationValue.SEARCH_INDEX_UPDATE_SQS_QUEUE_NAME)).thenReturn("");

        // Call the method under test
        try
        {
            searchIndexUpdateHelper.modifyTagsInSearchIndex(tagEntityList, SEARCH_INDEX_UPDATE_TYPE_UPDATE);
            fail("Should have thrown an IllegalStateException");
        }
        catch (IllegalStateException illegalStateException)
        {
            assertThat("Function is null.", illegalStateException.getMessage(),
                is("SQS queue name not found. Ensure the \"search.index.update.sqs.queue.name\" configuration entry is configured."));
        }

        // Verify the calls to external methods
        verify(jsonHelper, times(1)).objectToJson(searchIndexUpdateDto);
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.SEARCH_INDEX_UPDATE_JMS_LISTENER_ENABLED);
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.SEARCH_INDEX_UPDATE_SQS_QUEUE_NAME);
    }

    @Test
    public void testModifyTagssInSearchIndexBlankMessage()
    {
        // Create a tag entity list
        TagEntity tagEntity = new TagEntity();
        tagEntity.setId(1);
        List<TagEntity> tagEntityList = new ArrayList<>();
        tagEntityList.add(tagEntity);

        // Create a list of tag ids
        List<Integer> tagIds = new ArrayList<>();
        tagIds.add(tagEntity.getId());

        // Create a search index dto
        SearchIndexUpdateDto searchIndexUpdateDto = new SearchIndexUpdateDto(MESSAGE_TYPE_TAG_UPDATE, tagIds, SEARCH_INDEX_UPDATE_TYPE_UPDATE);

        // Mock the call to external methods
        when(jsonHelper.objectToJson(searchIndexUpdateDto)).thenReturn("");
        when(configurationHelper.getProperty(ConfigurationValue.SEARCH_INDEX_UPDATE_JMS_LISTENER_ENABLED)).thenReturn("true");
        when(configurationHelper.getProperty(ConfigurationValue.SEARCH_INDEX_UPDATE_SQS_QUEUE_NAME)).thenReturn("QUEUE NAME");

        // Call the method under test
        searchIndexUpdateHelper.modifyTagsInSearchIndex(tagEntityList, SEARCH_INDEX_UPDATE_TYPE_UPDATE);

        // Verify the calls to external methods
        verify(jsonHelper, times(1)).objectToJson(searchIndexUpdateDto);
        verify(configurationHelper, times(1)).getProperty(ConfigurationValue.SEARCH_INDEX_UPDATE_JMS_LISTENER_ENABLED);
    }
}
