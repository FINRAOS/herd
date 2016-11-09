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

import java.util.Set;

import org.finra.herd.model.api.xml.TagType;
import org.finra.herd.model.api.xml.TagTypeCreateRequest;
import org.finra.herd.model.api.xml.TagTypeKey;
import org.finra.herd.model.api.xml.TagTypeKeys;
import org.finra.herd.model.api.xml.TagTypeSearchRequest;
import org.finra.herd.model.api.xml.TagTypeSearchResponse;
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
     * Deletes a tag type.
     *
     * @param tagTypeKey the tag type key
     *
     * @return the deleted tag type
     */
    public TagType deleteTagType(TagTypeKey tagTypeKey);

    /**
     * Retrieve a tag type.
     *
     * @param tagTypeKey the tag type key
     *
     * @return the retrieved tag type
     */
    public TagType getTagType(TagTypeKey tagTypeKey);

    /**
     * Retrieves a list of tag type keys.
     *
     * @return the list of retrieved tag type keys
     */
    public TagTypeKeys getTagTypes();

    /**
     * Retrieves all tag types existing in the system. For each tag type entity, the endpoint returns tag type key by default along with any other top-level
     * elements as specified by the "fields" query string parameter. The list of tag types returned by the endpoint is sorted alphabetically by tag type's order
     * value ascending.
     *
     * @param request the tag type search request. The request does not take any search keys or filters
     * @param fields the field options for the tag type search response
     *
     * @return the tag type search response
     */
    public TagTypeSearchResponse searchTagTypes(TagTypeSearchRequest request, Set<String> fields);

    /**
     * Updates a tag type.
     *
     * @param request the tag type update request
     *
     * @return the updated tag type
     */
    public TagType updateTagType(TagTypeKey tagTypeKey, TagTypeUpdateRequest request);
}
