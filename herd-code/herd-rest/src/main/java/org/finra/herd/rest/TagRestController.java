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
    @Autowired
    private TagService tagService;

    /**
     * Creates a new tag.
     *
     * @param tagCreateRequest the information needed to create the tag
     *
     * @return the created tag type
     */
    @RequestMapping(value = "/tags", method = RequestMethod.POST, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_TAGS_POST)
    public Tag createTag(@RequestBody TagCreateRequest tagCreateRequest)
    {
        return tagService.createTag(tagCreateRequest);
    }

    /**
     * Gets an existing tag.
     *
     * @param tagTypeCode the tag type code
     * @param tagCode the tag code
     *
     * @return the tag
     */
    @RequestMapping(value = "/tags/tagTypes/{tagTypeCode}/tagCodes/{tagCode}", method = RequestMethod.GET, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_TAGS_GET)
    public Tag getTag(@PathVariable("tagTypeCode") String tagTypeCode, @PathVariable("tagCode") String tagCode)
    {
        TagKey tagKey = new TagKey(tagTypeCode, tagCode);
        return tagService.getTag(tagKey);
    }

    /**
     * Updates an existing tag.
     *
     * @param tagTypeCode      the tag type code
     * @param tagCode          the tag code
     * @param tagUpdateRequest the information needed to update the tag
     *
     * @return the updated tag
     */
    @RequestMapping(value = "/tags/tagTypes/{tagTypeCode}/tagCodes/{tagCode}", method = RequestMethod.PUT, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_TAGS_PUT)
    public Tag updateTag(@PathVariable("tagTypeCode") String tagTypeCode, @PathVariable("tagCode") String tagCode,
            @RequestBody TagUpdateRequest tagUpdateRequest)
    {
        TagKey tagKey = new TagKey(tagTypeCode, tagCode);
        return tagService.updateTag(tagKey, tagUpdateRequest);
    }

    /**
     * Deletes an existing tag.
     *
     * @param tagTypeCode the tag type code
     * @param tagCode the tag code
     *
     * @return the deleted tag
     */
    @RequestMapping(value = "/tags/tagTypes/{tagTypeCode}/tagCodes/{tagCode}", method = RequestMethod.DELETE, consumes = {"application/xml",
            "application/json"})
    @Secured(SecurityFunctions.FN_TAGS_DELETE)
    public Tag deleteTag(@PathVariable("tagTypeCode") String tagTypeCode, @PathVariable("tagCode") String tagCode)
    {
        TagKey tagKey = new TagKey(tagTypeCode, tagCode);
        return tagService.deleteTag(tagKey);
    }

    /**
     * Retrieves all associated tags for the specified tag type code.
     * When tagCode is null, return all tags of the tag type code, which has no parent (i.e. root tags).
     * When tagCode is provided, return all tags of the tag type code and whose parent tag code is tagCode.
     * 
     * @param tagTypeCode the tag type's code.
     * @param tagCode the parent tag code.
     *
     * @return all associated tags, with parent, itself and the children tags with has more children flag.
     */
    @RequestMapping(value = "/tags/tagTypes/{tagTypeCode}", method = RequestMethod.GET, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_TAGS_ALL_GET)
    public TagListResponse getTags(@PathVariable("tagTypeCode") String tagTypeCode, @RequestParam("tagCode") String tagCode)
    {       
        return tagService.getTags(tagTypeCode, tagCode);
    }
}
