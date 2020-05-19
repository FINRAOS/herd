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
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonParseException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.dao.helper.JsonHelper;
import org.finra.herd.model.api.xml.TagKey;
import org.finra.herd.model.jpa.TagEntity;
import org.finra.herd.service.AbstractServiceTest;

/**
 * This class tests functionality within the helper for tag related operations.
 */
public class TagHelperTest extends AbstractServiceTest
{
    @Mock
    private AlternateKeyHelper alternateKeyHelper;

    @Mock
    private JsonHelper jsonHelper;

    @InjectMocks
    private TagHelper tagHelper;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testExecuteFunctionForTagEntities()
    {
        // Create a list of tag entities.
        final List<TagEntity> tagEntities = Collections.unmodifiableList(Arrays
            .asList(tagDaoTestHelper.createTagEntity(TAG_TYPE, TAG_CODE, TAG_DISPLAY_NAME, TAG_DESCRIPTION),
                tagDaoTestHelper.createTagEntity(TAG_TYPE_2, TAG_CODE_2, TAG_DISPLAY_NAME_2, TAG_DESCRIPTION_2)));

        // Mock the external calls.
        when(jsonHelper.objectToJson(any())).thenReturn(JSON_STRING);

        // Execute a function for all tag entities.
        tagHelper.executeFunctionForTagEntities(SEARCH_INDEX_NAME, tagEntities, (indexName, id, json) -> { });

        // Verify the external calls.
        verify(jsonHelper, times(tagEntities.size())).objectToJson(any());
        verifyNoMoreInteractions(alternateKeyHelper, jsonHelper);
    }

    @Test
    public void testSafeObjectMapperWriteValueAsString()
    {
        // Create a tag entity.
        final TagEntity tagEntity = tagDaoTestHelper.createTagEntity(TAG_TYPE, TAG_CODE, TAG_DISPLAY_NAME, TAG_DESCRIPTION);

        // Mock the external calls.
        when(jsonHelper.objectToJson(any())).thenReturn(JSON_STRING);

        // Call the method being tested.
        String result = tagHelper.safeObjectMapperWriteValueAsString(tagEntity);

        // Verify the external calls.
        verify(jsonHelper).objectToJson(any());
        verifyNoMoreInteractions(alternateKeyHelper, jsonHelper);

        // Validate the returned object.
        assertEquals(JSON_STRING, result);
    }

    @Test
    public void testSafeObjectMapperWriteValueAsStringJsonParseException()
    {
        // Create a tag entity.
        final TagEntity tagEntity = tagDaoTestHelper.createTagEntity(TAG_TYPE, TAG_CODE, TAG_DISPLAY_NAME, TAG_DESCRIPTION);

        // Mock the external calls.
        when(jsonHelper.objectToJson(any()))
            .thenThrow(new IllegalStateException(new JsonParseException("Failed to Parse", new JsonLocation("SRC", 100L, 1, 2))));

        // Call the method being tested.
        String result = tagHelper.safeObjectMapperWriteValueAsString(tagEntity);

        // Verify the external calls.
        verify(jsonHelper).objectToJson(any());
        verifyNoMoreInteractions(alternateKeyHelper, jsonHelper);

        // Validate the returned object.
        assertEquals("", result);
    }

    @Test
    public void testValidateTagKey()
    {
        // Create a tag key.
        TagKey tagKey = new TagKey(TAG_TYPE, TAG_CODE);

        // Mock the external calls.
        when(alternateKeyHelper.validateStringParameter("tag type code", TAG_TYPE)).thenReturn(TAG_TYPE);
        when(alternateKeyHelper.validateStringParameter("tag code", TAG_CODE)).thenReturn(TAG_CODE);

        // Validate and trim a tag key.
        tagHelper.validateTagKey(tagKey);

        // Verify the external calls.
        verify(alternateKeyHelper).validateStringParameter("tag type code", TAG_TYPE);
        verify(alternateKeyHelper).validateStringParameter("tag code", TAG_CODE);
        verifyNoMoreInteractions(alternateKeyHelper, jsonHelper);

        // Validate the tag key.
        assertEquals(new TagKey(TAG_TYPE, TAG_CODE), tagKey);
    }

    @Test
    public void testValidateTagKeyTagKeyIsNull()
    {
        // Try to validate a null tag key.
        try
        {
            tagHelper.validateTagKey(null);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A tag key must be specified.", e.getMessage());
        }
    }
}
