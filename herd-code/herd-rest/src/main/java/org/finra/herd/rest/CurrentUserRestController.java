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
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import org.finra.herd.model.api.xml.UserAuthorizations;
import org.finra.herd.service.CurrentUserService;
import org.finra.herd.ui.constants.UiConstants;

/**
 * The REST controller that handles current user REST requests.
 */
@RestController
@RequestMapping(value = UiConstants.REST_URL_BASE, produces = {"application/xml", "application/json"})
@Api(tags = "Current User")
public class CurrentUserRestController
{
    public static final String CURRENT_USER_URI_PREFIX = "/currentUser";

    @Autowired
    private CurrentUserService currentUserService;

    /**
     * Gets all authorizations for the current user.
     *
     * @return the user authorizations
     */
    @RequestMapping(value = CURRENT_USER_URI_PREFIX, method = RequestMethod.GET)
    @PreAuthorize("isAuthenticated()")
    public UserAuthorizations getCurrentUser()
    {
        return currentUserService.getCurrentUser();
    }
}
