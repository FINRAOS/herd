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

import org.finra.herd.model.api.xml.NamespaceIamRoleAuthorization;
import org.finra.herd.model.api.xml.NamespaceIamRoleAuthorizationCreateRequest;
import org.finra.herd.model.api.xml.NamespaceIamRoleAuthorizationUpdateRequest;
import org.finra.herd.model.api.xml.NamespaceIamRoleAuthorizations;
import org.finra.herd.model.dto.SecurityFunctions;
import org.finra.herd.service.NamespaceIamRoleAuthorizationService;
import org.finra.herd.ui.constants.UiConstants;

/**
 * The REST controller for namespace IAM role authorizations.
 */
@RestController
@RequestMapping(value = UiConstants.REST_URL_BASE, produces = {"application/xml", "application/json"})
@Api(tags = "Namespace IAM Role Authorization")
public class NamespaceIamRoleAuthorizationRestController extends HerdBaseController
{
    @Autowired
    private NamespaceIamRoleAuthorizationService namespaceIamRoleAuthorizationService;

    /**
     * Authorizes a namespace to use IAM roles.
     *
     * @param request The namespace IAM role create request
     *
     * @return The namespace IAM role authorization
     */
    @RequestMapping(value = "/namespaceIamRoleAuthorizations", method = RequestMethod.POST, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_NAMESPACE_IAM_ROLE_AUTHORIZATIONS_POST)
    public NamespaceIamRoleAuthorization createNamespaceIamRoleAuthorization(@RequestBody NamespaceIamRoleAuthorizationCreateRequest request)
    {
        return namespaceIamRoleAuthorizationService.createNamespaceIamRoleAuthorization(request);
    }

    /**
     * Get the IAM roles that a namespace is authorized to use.
     *
     * @param namespace The namespace
     *
     * @return The namespace IAM role authorization
     */
    @RequestMapping(value = "/namespaceIamRoleAuthorizations/namespaces/{namespace}", method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_NAMESPACE_IAM_ROLE_AUTHORIZATIONS_GET)
    public NamespaceIamRoleAuthorization getNamespaceIamRoleAuthorization(@PathVariable("namespace") String namespace)
    {
        return namespaceIamRoleAuthorizationService.getNamespaceIamRoleAuthorization(namespace);
    }

    /**
     * Get a list of namespace IAM role authorizations.
     *
     * @return The list of namespace IAM role authorizations.
     */
    @RequestMapping(value = "/namespaceIamRoleAuthorizations", method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_NAMESPACE_IAM_ROLE_AUTHORIZATIONS_ALL_GET)
    public NamespaceIamRoleAuthorizations getNamespaceIamRoleAuthorizations()
    {
        return namespaceIamRoleAuthorizationService.getNamespaceIamRoleAuthorizations();
    }

    /**
     * Sets the authorizations a namespace has to use IAM roles.
     *
     * @param namespace The namespace to update authorizations
     * @param request The namespace IAM role update request
     *
     * @return The namespace IAM role authorization
     */
    @RequestMapping(value = "/namespaceIamRoleAuthorizations/namespaces/{namespace}", method = RequestMethod.PUT,
        consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_NAMESPACE_IAM_ROLE_AUTHORIZATIONS_PUT)
    public NamespaceIamRoleAuthorization updateNamespaceIamRoleAuthorization(@PathVariable("namespace") String namespace,
        @RequestBody NamespaceIamRoleAuthorizationUpdateRequest request)
    {
        return namespaceIamRoleAuthorizationService.updateNamespaceIamRoleAuthorization(namespace, request);
    }

    /**
     * Removes IAM roles a namespace has authorizations to use.
     *
     * @param namespace The namespace of the authorizations to remove
     *
     * @return The namespace IAM role authorization
     */
    @RequestMapping(value = "/namespaceIamRoleAuthorizations/namespaces/{namespace}", method = RequestMethod.DELETE)
    @Secured(SecurityFunctions.FN_NAMESPACE_IAM_ROLE_AUTHORIZATIONS_DELETE)
    public NamespaceIamRoleAuthorization deleteNamespaceIamRoleAuthorization(@PathVariable("namespace") String namespace)
    {
        return namespaceIamRoleAuthorizationService.deleteNamespaceIamRoleAuthorization(namespace);
    }
}
