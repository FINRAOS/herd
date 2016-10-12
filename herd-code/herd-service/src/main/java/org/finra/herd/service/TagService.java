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

import org.finra.herd.model.api.xml.Tag;
import org.finra.herd.model.api.xml.TagCreateRequest;
import org.finra.herd.model.api.xml.TagKey;
import org.finra.herd.model.api.xml.TagListResponse;
import org.finra.herd.model.api.xml.TagUpdateRequest;

/**
 * The tag service
 */
public interface TagService
{
    /**
     * Creates a new tag.
     *
     * @param tagCreateRequest the tag create request.
     *
     * @return the created tag.
     */
    public Tag createTag(TagCreateRequest tagCreateRequest);

    /**
     * Updates an existing tag.
     *
     * @param tagKey the tag's key.
     * @param tagUpdateRequest the tag update request.
     *
     * @return the updated tag.
     */
    public Tag updateTag(TagKey tagKey, TagUpdateRequest tagUpdateRequest);

    /**
     * Retrieves an existing tag.
     *
     * @param tagKey the tag's key.
     *
     * @return the tag.
     */
    public Tag getTag(TagKey tagKey);

    /**
     * Deletes an existing tag.
     *
     * @param tagKey the tag's key.
     *
     * @return the deleted tag.
     */
    public Tag deleteTag(TagKey tagKey);

    /**
     * Retrieves all associated tags for the specified tag type code.
     *
     * @param tagTypeCode the tag type's code.
     * @param tagCode the tag code.
     *
     * @return all associated tags.
     */
    public TagListResponse getTags(String tagTypeCode, String tagCode);
}
