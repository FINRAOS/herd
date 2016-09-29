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

import org.finra.herd.model.api.xml.TagType;
import org.finra.herd.model.api.xml.TagTypeCreateRequest;
import org.finra.herd.model.api.xml.TagTypeKey;
import org.finra.herd.model.api.xml.TagTypeKeys;
import org.finra.herd.model.api.xml.TagTypeUpdateRequest;

/**
 * The tag type service.
 */
public interface TagTypeService
{
    /**
     * Creates a new tag type.
     *
     * @param request the tag type create request
     *
     * @return the created tag type
     */
    public TagType createTagType(TagTypeCreateRequest request);

    /**
     * Updates a tag type.
     *
     * @param request the tag type update request
     *
     * @return the updated tag type
     */
    public TagType updateTagType(TagTypeKey tagTypeKey, TagTypeUpdateRequest request);

    /**
     * Retrieve a tag type.
     *
     * @param tagTypeKey the tag type key
     *
     * @return the retrieved tag type
     */
    public TagType getTagType(TagTypeKey tagTypeKey);

    /**
     * Deletes a tag type.
     *
     * @param tagTypeKey the tag type key
     *
     * @return the deleted tag type
     */
    public TagType deleteTagType(TagTypeKey tagTypeKey);

    /**
     * Retrieves a list of tag type keys.
     *
     * @return the list of retrieved tag type keys
     */
    public TagTypeKeys getTagTypes();
}
