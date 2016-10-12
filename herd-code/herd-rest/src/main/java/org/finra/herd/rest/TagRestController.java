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
import org.finra.herd.model.api.xml.*;
import org.finra.herd.model.dto.SecurityFunctions;
import org.finra.herd.service.TagService;
import org.finra.herd.ui.constants.UiConstants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.annotation.Secured;
import org.springframework.web.bind.annotation.*;

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
     * Get all tags by a tag type code.
     *
     * @param tagTypeCode the tag type code
     *
     * @return the list of tag keys
     */
    @RequestMapping(value = "/tags/tagTypes/{tagTypeCode}", method = RequestMethod.GET, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_TAGS_ALL_GET)
    public TagKeys getTags(@PathVariable("tagTypeCode") String tagTypeCode)
    {
        return tagService.getTags(tagTypeCode);
    }
}
