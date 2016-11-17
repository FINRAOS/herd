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

import java.util.Set;

import io.swagger.annotations.Api;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.annotation.Secured;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import org.finra.herd.model.api.xml.Tag;
import org.finra.herd.model.api.xml.TagCreateRequest;
import org.finra.herd.model.api.xml.TagKey;
import org.finra.herd.model.api.xml.TagListResponse;
import org.finra.herd.model.api.xml.TagSearchRequest;
import org.finra.herd.model.api.xml.TagSearchResponse;
import org.finra.herd.model.api.xml.TagUpdateRequest;
import org.finra.herd.model.dto.SecurityFunctions;
import org.finra.herd.service.TagService;
import org.finra.herd.ui.constants.UiConstants;

/**
 * The REST controller that handles tag REST requests.
 */
@RestController
@RequestMapping(value = UiConstants.REST_URL_BASE, produces = {"application/xml", "application/json"})
@Api(tags = "Tag")
public class TagRestController
{
    public final static String TAGS_URI_PREFIX = "/tags";

    @Autowired
    private TagService tagService;

    /**
     * Creates a new tag.
     *
     * @param tagCreateRequest the information needed to create the tag
     *
     * @return the created tag type
     */
    @RequestMapping(value = TAGS_URI_PREFIX, method = RequestMethod.POST, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_TAGS_POST)
    public Tag createTag(@RequestBody TagCreateRequest tagCreateRequest)
    {
        return tagService.createTag(tagCreateRequest);
    }

    /**
     * Deletes an existing tag.
     *
     * @param tagTypeCode the tag type code
     * @param tagCode the tag code
     *
     * @return the deleted tag
     */
    @RequestMapping(value = TAGS_URI_PREFIX + "/tagTypes/{tagTypeCode}/tagCodes/{tagCode}", method = RequestMethod.DELETE)
    @Secured(SecurityFunctions.FN_TAGS_DELETE)
    public Tag deleteTag(@PathVariable("tagTypeCode") String tagTypeCode, @PathVariable("tagCode") String tagCode)
    {
        TagKey tagKey = new TagKey(tagTypeCode, tagCode);
        return tagService.deleteTag(tagKey);
    }

    /**
     * Gets an existing tag.
     *
     * @param tagTypeCode the tag type code
     * @param tagCode the tag code
     *
     * @return the tag
     */
    @RequestMapping(value = TAGS_URI_PREFIX + "/tagTypes/{tagTypeCode}/tagCodes/{tagCode}", method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_TAGS_GET)
    public Tag getTag(@PathVariable("tagTypeCode") String tagTypeCode, @PathVariable("tagCode") String tagCode)
    {
        TagKey tagKey = new TagKey(tagTypeCode, tagCode);
        return tagService.getTag(tagKey);
    }

    /**
     * Retrieves all associated tags for the specified tag type code. When tagCode is null, return all tags of the tag type code, which has no parent (i.e. root
     * tags). When tagCode is provided, return all tags of the tag type code and whose parent tag code is tagCode.
     *
     * @param tagTypeCode the tag type's code.
     * @param tagCode the parent tag code.
     *
     * @return all associated tags, with parent, itself and the children tags with has more children flag.
     */
    @RequestMapping(value = TAGS_URI_PREFIX + "/tagTypes/{tagTypeCode}", method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_TAGS_ALL_GET)
    public TagListResponse getTags(@PathVariable("tagTypeCode") String tagTypeCode, @RequestParam(value = "tagCode", required = false) String tagCode)
    {
        return tagService.getTags(tagTypeCode, tagCode);
    }

    /**
     * Retrieves all tags existing in the system per specified search filters and keys. For each tag entity, this endpoint returns tag key by default along with
     * any other top-level elements as specified by the "fields" query string parameter. The list of tags returned by this endpoint is sorted alphabetically by
     * tag's display name ascending.
     *
     * @param request the tag search request. The request can only accept a single search filter and a single search key
     * @param fields the field options for the tag type search response. The valid field options are: displayName, description, parentTagKey, hasChildren
     *
     * @return the tag search response
     */
    @RequestMapping(value = TAGS_URI_PREFIX + "/search", method = RequestMethod.POST, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_TAGS_SEARCH_POST)
    public TagSearchResponse searchTags(@RequestBody TagSearchRequest request,
        @RequestParam(value = "fields", required = false, defaultValue = "") Set<String> fields)
    {
        return tagService.searchTags(request, fields);
    }

    /**
     * Updates an existing tag.
     *
     * @param tagTypeCode the tag type code
     * @param tagCode the tag code
     * @param tagUpdateRequest the information needed to update the tag
     *
     * @return the updated tag
     */
    @RequestMapping(value = TAGS_URI_PREFIX + "/tagTypes/{tagTypeCode}/tagCodes/{tagCode}", method = RequestMethod.PUT,
        consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_TAGS_PUT)
    public Tag updateTag(@PathVariable("tagTypeCode") String tagTypeCode, @PathVariable("tagCode") String tagCode,
        @RequestBody TagUpdateRequest tagUpdateRequest)
    {
        TagKey tagKey = new TagKey(tagTypeCode, tagCode);
        return tagService.updateTag(tagKey, tagUpdateRequest);
    }
}
