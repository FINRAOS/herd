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
import static org.junit.Assert.assertNotNull;

import org.springframework.stereotype.Component;

import org.finra.herd.model.api.xml.TagType;
import org.finra.herd.model.api.xml.TagTypeCreateRequest;
import org.finra.herd.model.api.xml.TagTypeKey;
import org.finra.herd.model.api.xml.TagTypeUpdateRequest;

@Component
public class TagTypeServiceTestHelper
{
    /**
     * Creates a tag type create request.
     *
     * @param tagTypeCode the tag type code
     * @param displayName the tag type display name
     * @param orderNumber the tag type ordering
     *
     * @return the newly created tag type create request
     */
    public TagTypeCreateRequest createTagTypeCreateRequest(String tagTypeCode, String displayName, int orderNumber)
    {
        TagTypeCreateRequest request = new TagTypeCreateRequest();
        request.setTagTypeKey(new TagTypeKey(tagTypeCode));
        request.setDisplayName(displayName);
        request.setTagTypeOrder(orderNumber);
        return request;
    }

    /**
     * Creates a tag type update request.
     *
     * @param displayName the tag type display name
     * @param orderNumber the tag type ordering
     *
     * @return the newly created tag type create request
     */
    public TagTypeUpdateRequest createTagTypeUpdateRequest(String displayName, int orderNumber)
    {
        TagTypeUpdateRequest request = new TagTypeUpdateRequest();
        request.setDisplayName(displayName);
        request.setTagTypeOrder(orderNumber);
        return request;
    }

    /**
     * Validates tag type contents against specified parameters.
     *
     * @param expectedTagTypeCode the expected tag type code
     * @param actualTagType the tag type object instance to be validated
     */
    public void validateTagType(int tagTypeId, String expectedTagTypeCode, String expectedDisplayName, int expectedOrderNumber, TagType actualTagType)
    {
        assertNotNull(actualTagType);
        assertEquals(new TagType(tagTypeId, new TagTypeKey(expectedTagTypeCode), expectedDisplayName, expectedOrderNumber), actualTagType);
    }
}
