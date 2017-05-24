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

import java.util.Arrays;
import java.util.Set;

import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.model.api.xml.TagType;
import org.finra.herd.model.api.xml.TagTypeCreateRequest;
import org.finra.herd.model.api.xml.TagTypeKey;
import org.finra.herd.model.api.xml.TagTypeKeys;
import org.finra.herd.model.api.xml.TagTypeSearchRequest;
import org.finra.herd.model.api.xml.TagTypeSearchResponse;
import org.finra.herd.model.api.xml.TagTypeUpdateRequest;
import org.finra.herd.service.TagTypeService;
import org.finra.herd.service.impl.TagTypeServiceImpl;

/**
 * This class tests various functionality within the tag type REST controller.
 */
public class TagTypeRestControllerTest extends AbstractRestTest
{
    @Mock
    private TagTypeService tagTypeService;

    @InjectMocks
    private TagTypeRestController tagTypeRestController;

    @Before()
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCreateTagType() throws Exception
    {
        TagTypeCreateRequest request = new TagTypeCreateRequest(new TagTypeKey(TAG_TYPE), TAG_TYPE_DISPLAY_NAME, TAG_TYPE_ORDER, TAG_TYPE_DESCRIPTION);

        TagType tagType = new TagType(new TagTypeKey(TAG_TYPE), TAG_TYPE_DISPLAY_NAME, TAG_TYPE_ORDER, TAG_TYPE_DESCRIPTION);

        when(tagTypeService.createTagType(request)).thenReturn(tagType);
        // Create a tag type.
        TagType resultTagType = tagTypeRestController
            .createTagType(new TagTypeCreateRequest(new TagTypeKey(TAG_TYPE), TAG_TYPE_DISPLAY_NAME, TAG_TYPE_ORDER, TAG_TYPE_DESCRIPTION));

        // Verify the external calls.
        verify(tagTypeService).createTagType(request);
        verifyNoMoreInteractions(tagTypeService);

        // Validate the returned object.
        assertEquals(tagType, resultTagType);
    }

    @Test
    public void testDeleteTagType() throws Exception
    {
        TagTypeKey tagTypeKey = new TagTypeKey(TAG_TYPE);

        TagType tagType = new TagType(new TagTypeKey(TAG_TYPE), TAG_TYPE_DISPLAY_NAME, TAG_TYPE_ORDER, TAG_TYPE_DESCRIPTION);
        when(tagTypeService.deleteTagType(tagTypeKey)).thenReturn(tagType);

        // Delete this tag type.
        TagType deletedTagType = tagTypeRestController.deleteTagType(TAG_TYPE);

        // Verify the external calls.
        verify(tagTypeService).deleteTagType(tagTypeKey);
        verifyNoMoreInteractions(tagTypeService);

        // Validate the returned object.
        assertEquals(tagType, deletedTagType);
    }

    @Test
    public void testGetTagType() throws Exception
    {
        TagTypeKey tagTypeKey = new TagTypeKey(TAG_TYPE);
        TagType tagType = new TagType(new TagTypeKey(TAG_TYPE), TAG_TYPE_DISPLAY_NAME, TAG_TYPE_ORDER, TAG_TYPE_DESCRIPTION);

        when(tagTypeService.getTagType(tagTypeKey)).thenReturn(tagType);

        // Retrieve the tag type.
        TagType resultTagType = tagTypeRestController.getTagType(TAG_TYPE);

        // Verify the external calls.
        verify(tagTypeService).getTagType(tagTypeKey);
        verifyNoMoreInteractions(tagTypeService);

        // Validate the returned object.
        assertEquals(tagType, resultTagType);
    }

    @Test
    public void testGetTagTypes() throws Exception
    {
        TagTypeKeys tagTypeKeys = new TagTypeKeys(Arrays.asList(new TagTypeKey(TAG_TYPE)));

        when(tagTypeService.getTagTypes()).thenReturn(tagTypeKeys);

        // Retrieve a list of tag type keys.
        TagTypeKeys resultTagTypeKeys = tagTypeRestController.getTagTypes();

        // Verify the external calls.
        verify(tagTypeService).getTagTypes();
        verifyNoMoreInteractions(tagTypeService);

        // Validate the returned object.
        assertEquals(tagTypeKeys, resultTagTypeKeys);
    }

    @Test
    public void testSearchTagTypes()
    {
        TagTypeSearchResponse tagTypeSearchResponse = new TagTypeSearchResponse(Arrays
            .asList(new TagType(new TagTypeKey(TAG_TYPE_2), TAG_TYPE_DISPLAY_NAME_2, TAG_TYPE_ORDER, TAG_TYPE_DESCRIPTION_2),
                new TagType(new TagTypeKey(TAG_TYPE), TAG_TYPE_DISPLAY_NAME, TAG_TYPE_ORDER_2, TAG_TYPE_DESCRIPTION)));

        Set<String> searchFields =
            Sets.newHashSet(TagTypeServiceImpl.DISPLAY_NAME_FIELD, TagTypeServiceImpl.TAG_TYPE_ORDER_FIELD, TagTypeServiceImpl.DESCRIPTION_FIELD);

        when(tagTypeService.searchTagTypes(new TagTypeSearchRequest(), searchFields)).thenReturn(tagTypeSearchResponse);

        // Search tag types.
        TagTypeSearchResponse resultTagTypeSearchResponse = tagTypeRestController.searchTagTypes(new TagTypeSearchRequest(), searchFields);

        // Verify the external calls.
        verify(tagTypeService).searchTagTypes(new TagTypeSearchRequest(), searchFields);
        verifyNoMoreInteractions(tagTypeService);

        // Validate the returned object.
        assertEquals(tagTypeSearchResponse, resultTagTypeSearchResponse);
    }

    @Test
    public void testUpdateTagType() throws Exception
    {
        TagTypeKey tagTypeKey = new TagTypeKey(TAG_TYPE);

        TagType tagType = new TagType(new TagTypeKey(TAG_TYPE), TAG_TYPE_DISPLAY_NAME_2, TAG_TYPE_ORDER_2, TAG_TYPE_DESCRIPTION_2);
        TagTypeUpdateRequest request = new TagTypeUpdateRequest(TAG_TYPE_DISPLAY_NAME_2, TAG_TYPE_ORDER_2, TAG_TYPE_DESCRIPTION_2);

        when(tagTypeService.updateTagType(tagTypeKey, request)).thenReturn(tagType);

        // Retrieve the tag type.
        TagType resultTagType = tagTypeRestController.updateTagType(TAG_TYPE, request);

        // Validate the returned object.
        assertEquals(tagType, resultTagType);
    }
}
