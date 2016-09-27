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
import org.springframework.web.bind.annotation.RestController;

import org.finra.herd.model.api.xml.TagType;
import org.finra.herd.model.api.xml.TagTypeCreateRequest;
import org.finra.herd.model.api.xml.TagTypeKey;
import org.finra.herd.model.api.xml.TagTypeKeys;
import org.finra.herd.model.api.xml.TagTypeUpdateRequest;
import org.finra.herd.model.dto.SecurityFunctions;
import org.finra.herd.service.TagTypeService;
import org.finra.herd.ui.constants.UiConstants;

/**
 * The REST controller that handles tag type REST requests.
 */
@RestController
@RequestMapping(value = UiConstants.REST_URL_BASE, produces = {"application/xml", "application/json"})
@Api(tags = "Tag Type")
public class TagTypeRestController
{
    @Autowired
    private TagTypeService tagTypeService;

    /**
     * Creates a new tag type.
     *
     * @param request the information needed to create the tag type
     *
     * @return the created tag type
     */
    @RequestMapping(value = "/tagTypes", method = RequestMethod.POST, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_TAG_TYPES_POST)
    public TagType createTagType(@RequestBody TagTypeCreateRequest request)
    {
        return tagTypeService.createTagType(request);
    }

    /**
     * Updates an existing tag type.
     *
     * @param tagTypeCode the tag type code
     * @param request the information needed to update the tag type
     *
     * @return the updated tag type
     */
    @RequestMapping(value = "/tagTypes/{tagTypeCode}", method = RequestMethod.PUT,
        consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_TAG_TYPES_PUT)
    public TagType updateTagType(@PathVariable("tagTypeCode") String tagTypeCode, @RequestBody TagTypeUpdateRequest request) throws Exception
    {
        return tagTypeService.updateTagType(new TagTypeKey(tagTypeCode), request);
    }

    /**
     * Gets an existing tag type by tag type code.
     *
     * @param tagTypeCode the tag type code
     *
     * @return the retrieved tag type
     */
    @RequestMapping(value = "/tagTypes/{tagTypeCode}", method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_TAG_TYPES_GET)
    public TagType getTagType(@PathVariable("tagTypeCode") String tagTypeCode)
    {
        return tagTypeService.getTagType(new TagTypeKey(tagTypeCode));
    }

    /**
     * Deletes an existing tag type by tag type code.
     *
     * @param tagTypeCode the tag type code
     *
     * @return the tag type that got deleted
     */
    @RequestMapping(value = "/tagTypes/{tagTypeCode}", method = RequestMethod.DELETE)
    @Secured(SecurityFunctions.FN_TAG_TYPES_DELETE)
    public TagType deleteTagType(@PathVariable("tagTypeCode") String tagTypeCode)
    {
        return tagTypeService.deleteTagType(new TagTypeKey(tagTypeCode));
    }

    /**
     * Gets a list of tag type keys for all tag types defined in the system.
     *
     * @return the list of tag type keys
     */
    @RequestMapping(value = "/tagTypes", method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_TAG_TYPES_ALL_GET)
    public TagTypeKeys getTagTypes()
    {
        return tagTypeService.getTagTypes();
    }
}
