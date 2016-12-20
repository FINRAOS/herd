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
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import org.finra.herd.model.api.xml.SubjectMatterExpert;
import org.finra.herd.model.api.xml.SubjectMatterExpertKey;
import org.finra.herd.model.dto.SecurityFunctions;
import org.finra.herd.service.SubjectMatterExpertService;
import org.finra.herd.ui.constants.UiConstants;

/**
 * The REST controller that handles business object definition subject matter expert requests.
 */
@RestController
@RequestMapping(value = UiConstants.REST_URL_BASE, produces = {"application/xml", "application/json"})
@Api(tags = "Subject Matter Expert")
public class SubjectMatterExpertRestController extends HerdBaseController
{
    public static final String SUBJECT_MATTER_EXPERTS_URI_PREFIX = "/subjectMatterExperts";

    @Autowired
    private SubjectMatterExpertService subjectMatterExpertService;

    /**
     * Gets information for the specified subject matter expert.
     *
     * @param userId the user id of the subject matter expert
     *
     * @return the subject matter expert information
     */
    @RequestMapping(value = SUBJECT_MATTER_EXPERTS_URI_PREFIX + "/{userId}", method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_SUBJECT_MATTER_EXPERTS_GET)
    public SubjectMatterExpert getSubjectMatterExpert(@PathVariable("userId") String userId)
    {
        return subjectMatterExpertService.getSubjectMatterExpert(new SubjectMatterExpertKey(userId));
    }
}
