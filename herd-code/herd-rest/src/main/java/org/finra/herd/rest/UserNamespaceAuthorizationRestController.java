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

import org.finra.herd.model.api.xml.UserNamespaceAuthorization;
import org.finra.herd.model.api.xml.UserNamespaceAuthorizationCreateRequest;
import org.finra.herd.model.api.xml.UserNamespaceAuthorizationKey;
import org.finra.herd.model.api.xml.UserNamespaceAuthorizationUpdateRequest;
import org.finra.herd.model.api.xml.UserNamespaceAuthorizations;
import org.finra.herd.model.dto.SecurityFunctions;
import org.finra.herd.service.UserNamespaceAuthorizationService;
import org.finra.herd.ui.constants.UiConstants;

/**
 * The REST controller that handles user namespace authorization REST requests.
 */
@RestController
@RequestMapping(value = UiConstants.REST_URL_BASE, produces = {"application/xml", "application/json"})
@Api(tags = "User Namespace Authorization")
public class UserNamespaceAuthorizationRestController extends HerdBaseController
{
    public static final String USER_NAMESPACE_AUTHORIZATIONS_URI_PREFIX = "/userNamespaceAuthorizations";

    @Autowired
    private UserNamespaceAuthorizationService userNamespaceAuthorizationService;

    /**
     * <p>Creates a new user namespace authorization.</p> <p>The user ID field may be prefixed with a wildcard token character "*" to authorize multiple users
     * access to the namespace. The wildcard only works for as a prefix to match the suffix of the user ID. If the wildcard appears anywhere other than the
     * prefix, the user ID must match as-is. For example:</p> <ul> <li>john.doe@domain.com - only authorizes user with ID "john.doe@domain.com"</li>
     * <li>*@domain.com - authorizes users that has the suffix "@domain.com"</li> <li>* - authorizes all users</li> <li>john.doe* - only authorizes user with ID
     * "john.doe*"</li> </ul> <p>Requires GRANT permission on namespace</p>
     *
     * @param request the information needed to create the user namespace authorization
     *
     * @return the created user namespace authorization
     */
    @RequestMapping(value = USER_NAMESPACE_AUTHORIZATIONS_URI_PREFIX, method = RequestMethod.POST, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_USER_NAMESPACE_AUTHORIZATIONS_POST)
    public UserNamespaceAuthorization createUserNamespaceAuthorization(@RequestBody UserNamespaceAuthorizationCreateRequest request)
    {
        return userNamespaceAuthorizationService.createUserNamespaceAuthorization(request);
    }

    /**
     * Updates an existing user namespace authorization by key. <p>Requires GRANT permission on namespace</p>
     *
     * @param userId the user id
     * @param namespace the namespace
     * @param request the information needed to update the user namespace authorization
     *
     * @return the updated user namespace authorization
     */
    @RequestMapping(value = USER_NAMESPACE_AUTHORIZATIONS_URI_PREFIX + "/userIds/{userId}/namespaces/{namespace}",
        method = RequestMethod.PUT,
        consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_USER_NAMESPACE_AUTHORIZATIONS_PUT)
    public UserNamespaceAuthorization updateUserNamespaceAuthorization(@PathVariable("userId") String userId, @PathVariable("namespace") String namespace,
        @RequestBody UserNamespaceAuthorizationUpdateRequest request)
    {
        return userNamespaceAuthorizationService.updateUserNamespaceAuthorization(new UserNamespaceAuthorizationKey(userId, namespace), request);
    }

    /**
     * Gets an existing user namespace authorization by key. <p>Requires READ permission on namespace</p>
     *
     * @param userId the user id
     * @param namespace the namespace
     *
     * @return the retrieved user namespace authorization
     */
    @RequestMapping(value = USER_NAMESPACE_AUTHORIZATIONS_URI_PREFIX + "/userIds/{userId}/namespaces/{namespace}",
        method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_USER_NAMESPACE_AUTHORIZATIONS_GET)
    public UserNamespaceAuthorization getUserNamespaceAuthorization(@PathVariable("userId") String userId, @PathVariable("namespace") String namespace)
    {
        return userNamespaceAuthorizationService.getUserNamespaceAuthorization(new UserNamespaceAuthorizationKey(userId, namespace));
    }

    /**
     * Deletes an existing user namespace authorization by key. <p>Requires GRANT permission on namespace</p>
     *
     * @param userId the user id
     * @param namespace the namespace
     *
     * @return the deleted user namespace authorization
     */
    @RequestMapping(value = USER_NAMESPACE_AUTHORIZATIONS_URI_PREFIX + "/userIds/{userId}/namespaces/{namespace}",
        method = RequestMethod.DELETE)
    @Secured(SecurityFunctions.FN_USER_NAMESPACE_AUTHORIZATIONS_DELETE)
    public UserNamespaceAuthorization deleteUserNamespaceAuthorization(@PathVariable("userId") String userId, @PathVariable("namespace") String namespace)
    {
        return userNamespaceAuthorizationService.deleteUserNamespaceAuthorization(new UserNamespaceAuthorizationKey(userId, namespace));
    }

    /**
     * Deletes all existing user namespace authorizations for the specified user.
     *
     * @param userId the user id
     *
     * @return the list of deleted user namespace authorizations
     */
    @RequestMapping(value = USER_NAMESPACE_AUTHORIZATIONS_URI_PREFIX + "/userIds/{userId}", method = RequestMethod.DELETE)
    @Secured(SecurityFunctions.FN_USER_NAMESPACE_AUTHORIZATIONS_BY_USERID_DELETE)
    public UserNamespaceAuthorizations deleteUserNamespaceAuthorizationsByUserId(@PathVariable("userId") String userId)
    {
        return userNamespaceAuthorizationService.deleteUserNamespaceAuthorizationsByUserId(userId);
    }

    /**
     * Gets a list of user namespace authorizations for the specified user.
     *
     * @param userId the user id
     *
     * @return the list of user namespace authorizations
     */
    @RequestMapping(value = USER_NAMESPACE_AUTHORIZATIONS_URI_PREFIX + "/userIds/{userId}", method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_USER_NAMESPACE_AUTHORIZATIONS_BY_USERID_GET)
    public UserNamespaceAuthorizations getUserNamespaceAuthorizationsByUserId(@PathVariable("userId") String userId)
    {
        return userNamespaceAuthorizationService.getUserNamespaceAuthorizationsByUserId(userId);
    }

    /**
     * Gets a list of user namespace authorizations for the specified namespace.
     *
     * @param namespace the namespace
     *
     * @return the list of user namespace authorizations
     */
    @RequestMapping(value = USER_NAMESPACE_AUTHORIZATIONS_URI_PREFIX + "/namespaces/{namespace}", method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_USER_NAMESPACE_AUTHORIZATIONS_BY_NAMESPACE_GET)
    public UserNamespaceAuthorizations getUserNamespaceAuthorizationsByNamespace(@PathVariable("namespace") String namespace)
    {
        return userNamespaceAuthorizationService.getUserNamespaceAuthorizationsByNamespace(namespace);
    }
}
