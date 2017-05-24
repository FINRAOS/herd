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
package org.finra.herd.rest;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.core.HerdDateUtils;
import org.finra.herd.model.api.xml.Tag;
import org.finra.herd.model.api.xml.TagChild;
import org.finra.herd.model.api.xml.TagCreateRequest;
import org.finra.herd.model.api.xml.TagKey;
import org.finra.herd.model.api.xml.TagListResponse;
import org.finra.herd.model.api.xml.TagSearchFilter;
import org.finra.herd.model.api.xml.TagSearchKey;
import org.finra.herd.model.api.xml.TagSearchRequest;
import org.finra.herd.model.api.xml.TagSearchResponse;
import org.finra.herd.model.api.xml.TagUpdateRequest;
import org.finra.herd.service.TagService;
import org.finra.herd.service.impl.TagServiceImpl;

/**
 * This class tests various functionality within the Tag REST controller
 */
public class TagRestControllerTest extends AbstractRestTest
{
    @Mock
    private TagService tagService;

    @InjectMocks
    private TagRestController tagRestController;

    @Before()
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCreateTag()
    {
        TagKey tagKey = new TagKey(TAG_TYPE, TAG_CODE);
        TagCreateRequest request = new TagCreateRequest(tagKey, TAG_DISPLAY_NAME, TAG_DESCRIPTION, NO_PARENT_TAG_KEY);

        Tag tag = getNewTag(tagKey);

        when(tagService.createTag(request)).thenReturn(tag);
        // Create a tag.
        Tag resultTag = tagRestController.createTag(request);

        // Verify the external calls.
        verify(tagService).createTag(request);
        verifyNoMoreInteractions(tagService);

        // Validate the returned object.
        assertEquals(tag, resultTag);
    }

    @Test
    public void testDeleteTag()
    {
        // Create a tag key.
        TagKey tagKey = new TagKey(TAG_TYPE, TAG_CODE);

        Tag tag = getNewTag(tagKey);

        when(tagService.deleteTag(tagKey)).thenReturn(tag);
        // Delete this tag.
        Tag deletedTag = tagRestController.deleteTag(TAG_TYPE, TAG_CODE);

        // Verify the external calls.
        verify(tagService).deleteTag(tagKey);
        verifyNoMoreInteractions(tagService);

        // Validate the returned object.
        assertEquals(tag, deletedTag);
    }

    @Test
    public void testGetTag()
    {
        TagKey tagKey = new TagKey(TAG_TYPE, TAG_CODE);
        Tag tag = getNewTag(tagKey);

        when(tagService.getTag(tagKey)).thenReturn(tag);

        // Retrieve the tag.
        Tag resultTag = tagRestController.getTag(TAG_TYPE, TAG_CODE);

        // Verify the external calls.
        verify(tagService).getTag(tagKey);
        verifyNoMoreInteractions(tagService);

        // Validate the returned object.
        assertEquals(tag, resultTag);
    }

    @Test
    public void testGetTags()
    {
        TagKey tagKey = new TagKey(TAG_TYPE, TAG_CODE);
        List<TagChild> tagChildren = new ArrayList<>();
        tagChildren.add(new TagChild(new TagKey(TAG_TYPE, TAG_CODE), false));
        tagChildren.add(new TagChild(new TagKey(TAG_TYPE, TAG_CODE_2), false));

        TagListResponse tagListResponse = new TagListResponse();
        tagListResponse.setTagChildren(tagChildren);

        when(tagService.getTags(TAG_TYPE, TAG_CODE)).thenReturn(tagListResponse);
        // Retrieve the tag.
        TagListResponse resultTagKeys = tagRestController.getTags(TAG_TYPE, TAG_CODE);

        // Verify the external calls.
        verify(tagService).getTags(TAG_TYPE, TAG_CODE);
        verifyNoMoreInteractions(tagService);

        // Validate the returned object.
        assertEquals(tagListResponse, resultTagKeys);
        ;
    }

    @Test
    public void testSearchTags()
    {
        TagSearchResponse tagSearchResponse = new TagSearchResponse(Arrays.asList(
            new Tag(NO_ID, new TagKey(TAG_TYPE, TAG_CODE_3), TAG_DISPLAY_NAME_2, TAG_DESCRIPTION_3, NO_USER_ID, NO_USER_ID, NO_UPDATED_TIME,
                new TagKey(TAG_TYPE, TAG_CODE), TAG_HAS_NO_CHILDREN),
            new Tag(NO_ID, new TagKey(TAG_TYPE, TAG_CODE_2), TAG_DISPLAY_NAME_3, TAG_DESCRIPTION_2, NO_USER_ID, NO_USER_ID, NO_UPDATED_TIME,
                new TagKey(TAG_TYPE, TAG_CODE), TAG_HAS_NO_CHILDREN)));

        Set<String> searchFields = Sets.newHashSet(TagServiceImpl.DISPLAY_NAME_FIELD, TagServiceImpl.DESCRIPTION_FIELD, TagServiceImpl.PARENT_TAG_KEY_FIELD,
            TagServiceImpl.HAS_CHILDREN_FIELD);
        TagSearchRequest tagSearchRequest =
            new TagSearchRequest(Arrays.asList(new TagSearchFilter(Arrays.asList(new TagSearchKey(TAG_TYPE, TAG_CODE, NO_IS_PARENT_TAG_NULL_FLAG)))));

        when(tagService.searchTags(tagSearchRequest, searchFields)).thenReturn(tagSearchResponse);

        // Search the tags.
        TagSearchResponse resultTagSearchResponse = tagRestController.searchTags(tagSearchRequest, searchFields);

        // Verify the external calls.
        verify(tagService).searchTags(tagSearchRequest, searchFields);
        verifyNoMoreInteractions(tagService);

        // Validate the returned object.
        assertEquals(tagSearchResponse, resultTagSearchResponse);
    }

    @Test
    public void testUpdateTag()
    {
        // Create a parent tag key.
        TagKey parentTagKey = new TagKey(TAG_TYPE, TAG_CODE);
        TagUpdateRequest request = new TagUpdateRequest(TAG_DISPLAY_NAME_3, TAG_DESCRIPTION_3, parentTagKey);

        // Create a tag key.
        TagKey tagKey = new TagKey(TAG_TYPE, TAG_CODE_2);
        Tag tag = getNewTag(tagKey);

        when(tagService.updateTag(tagKey, request)).thenReturn(tag);
        // Update the tag.
        Tag updatedTag = tagRestController.updateTag(TAG_TYPE, TAG_CODE_2, request);

        // Verify the external calls.
        verify(tagService).updateTag(tagKey, request);
        verifyNoMoreInteractions(tagService);

        // Validate the returned object.
        assertEquals(tag, updatedTag);
        ;
    }

    private Tag getNewTag(TagKey tagKey)
    {
        Calendar cal = Calendar.getInstance();
        Date createdTime = cal.getTime();
        Date updatedTime = cal.getTime();
        String createdBy = "some test";
        String updatedBy = "some test 2";

        Tag tag = new Tag(100, tagKey, TAG_DISPLAY_NAME, TAG_DESCRIPTION, createdBy, updatedBy, HerdDateUtils.getXMLGregorianCalendarValue(createdTime),
            NO_PARENT_TAG_KEY, NO_TAG_HAS_CHILDREN_FLAG);

        return tag;
    }
}
