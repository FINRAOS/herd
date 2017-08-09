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
import java.util.concurrent.Future;

import org.finra.herd.model.api.xml.Tag;
import org.finra.herd.model.api.xml.TagCreateRequest;
import org.finra.herd.model.api.xml.TagKey;
import org.finra.herd.model.api.xml.TagListResponse;
import org.finra.herd.model.api.xml.TagSearchRequest;
import org.finra.herd.model.api.xml.TagSearchResponse;
import org.finra.herd.model.api.xml.TagUpdateRequest;
import org.finra.herd.model.dto.SearchIndexUpdateDto;

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
     * Deletes an existing tag.
     *
     * @param tagKey the tag's key.
     *
     * @return the deleted tag.
     */
    public Tag deleteTag(TagKey tagKey);

    /**
     * Retrieves an existing tag.
     *
     * @param tagKey the tag's key.
     *
     * @return the tag.
     */
    public Tag getTag(TagKey tagKey);

    /**
     * Retrieves all associated tags for the specified tag type code.
     * <p/>
     * When tagCode is null, return all tags of the tag type code.
     * <p/>
     * When tagCode is provided, return all tags of the tag type code and whose parent tag code is tagCode.
     *
     * @param tagTypeCode the tag type's code.
     * @param tagCode the tag code.
     *
     * @return all associated tags, with parent, itself and the children tags with has more children flag.
     */
    public TagListResponse getTags(String tagTypeCode, String tagCode);

    /**
     * Retrieves all tags existing in the system per specified search filters and keys. For each tag entity, this endpoint returns tag key by default along with
     * any other top-level elements as specified by the "fields" query string parameter. The list of tags returned by this endpoint is sorted alphabetically by
     * tag's display name ascending.
     *
     * @param request the tag search request. The request can only accept at most one search filter and a single search key
     * @param fields the field options for the tag type search response
     *
     * @return the tag search response
     */
    public TagSearchResponse searchTags(TagSearchRequest request, Set<String> fields);

    /**
     * Updates the search index document representation of the tag.
     *
     * @param searchIndexUpdateDto the SearchIndexUpdateDto object
     */
    public void updateSearchIndexDocumentTag(SearchIndexUpdateDto searchIndexUpdateDto);

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
     * Checks the count of tags in the database against the count of tags in the index.
     * @param indexName the name of the index
     * @return boolean value true for valid, false otherwise
     */
    public boolean indexSizeCheckValidationTags(String indexName);

    /**
     * Spot check a random percentage of tags in the search index
     * @param indexName the name of the index
     * @return boolean value true for valid, false otherwise
     */
    public boolean indexSpotCheckPercentageValidationTags(String indexName);

    /**
     * Spot check the most recent tags in the search index
     * @param indexName the name of the index
     * @return boolean value true for valid, false otherwise
     */
    public boolean indexSpotCheckMostRecentValidationTags(String indexName);

    /**
     * Validate that the search index contains all tags
     * @param indexName the name of the index
     * @return result of an asynchronous computation
     */
    public Future<Void> indexValidateAllTags(String indexName);
}
