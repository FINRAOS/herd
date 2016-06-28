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
package org.finra.herd.service.helper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

import org.finra.herd.model.api.xml.NamespaceAuthorization;
import org.finra.herd.model.api.xml.NamespacePermissionEnum;
import org.finra.herd.model.dto.ApplicationUser;
import org.finra.herd.model.dto.SecurityUserWrapper;

/**
 * Helper for checking permissions based on given parameters.
 */
@Component
public class NamespaceSecurityHelper
{
    private static final Logger LOGGER = LoggerFactory.getLogger(NamespaceSecurityHelper.class);

    /**
     * Checks the current user's permissions against the given object which may represent a single or multiple namespaces. Allowed types are String or
     * Collection of String.
     *
     * @param object The string or collection of strings which represents the namespace
     * @param permissions The set of permissions the current user must have for the given namespace(s)
     */
    public void checkPermission(Object object, NamespacePermissionEnum[] permissions)
    {
        List<AccessDeniedException> accessDeniedExceptions = new ArrayList<>();
        checkPermission(object, permissions, accessDeniedExceptions);

        if (!accessDeniedExceptions.isEmpty())
        {
            throw getAccessDeniedException(accessDeniedExceptions);
        }
    }

    /**
     * Checks the current user's permissions against the given namespace.
     *
     * @param namespace The namespace
     * @param permissions The permissions the current user must have for the given namespace
     */
    public void checkPermission(String namespace, NamespacePermissionEnum[] permissions)
    {
        // Skip the permission check if there is no authentication or namespace is not specified.
        if (!isAuthenticated() || StringUtils.isBlank(namespace))
        {
            return;
        }

        // Check if the current user is authorized to the given namespace and has the given permissions.
        ApplicationUser applicationUser = getApplicationUser();
        if (applicationUser != null && applicationUser.getNamespaceAuthorizations() != null)
        {
            for (NamespaceAuthorization currentUserAuthorization : applicationUser.getNamespaceAuthorizations())
            {
                List<NamespacePermissionEnum> currentUserNamespacePermissions = currentUserAuthorization.getNamespacePermissions();
                if (currentUserNamespacePermissions == null)
                {
                    currentUserNamespacePermissions = Collections.emptyList();
                }

                if (StringUtils.equalsIgnoreCase(currentUserAuthorization.getNamespace(), namespace.trim()) &&
                    currentUserNamespacePermissions.containsAll(Arrays.asList(permissions)))
                {
                    return;
                }
            }
        }

        // If we got here that means that the current user is not authorized to access the given namespace.
        String namespaceTrimmed = namespace.trim();
        LOGGER.warn(String
            .format("User does not have permission(s) to the namespace. %s namespace=\"%s\" permissions=\"%s\"", applicationUser, namespaceTrimmed,
                Arrays.asList(permissions)));
        if (applicationUser != null)
        {
            throw new AccessDeniedException(String
                .format("User \"%s\" does not have \"%s\" permission(s) to the namespace \"%s\"", applicationUser.getUserId(), Arrays.asList(permissions),
                    namespaceTrimmed));
        }
        else
        {
            throw new AccessDeniedException(
                String.format("Current user does not have \"%s\" permission(s) to the namespace \"%s\"", Arrays.asList(permissions), namespaceTrimmed));
        }
    }

    /**
     * Constructs a new access denied exception by concatenating the given list of exceptions.
     *
     * @param accessDeniedExceptions List of exceptions to concatenate
     *
     * @return A new AccessDeniedException
     */
    public AccessDeniedException getAccessDeniedException(List<AccessDeniedException> accessDeniedExceptions)
    {
        StringBuilder errorMessageBuilder = new StringBuilder();
        for (AccessDeniedException accessDeniedException : accessDeniedExceptions)
        {
            errorMessageBuilder.append(String.format("%s%n", accessDeniedException.getMessage()));
        }
        return new AccessDeniedException(errorMessageBuilder.toString().trim());
    }

    /**
     * Gets a set of namespace codes which the current user is authorized for the given permissions.
     *
     * @param permissions List of permissions to query
     *
     * @return Set of namespace codes
     */
    public Set<String> getAuthorizedNamespaces(NamespacePermissionEnum... permissions)
    {
        Set<String> namespaces = new HashSet<>();
        if (SecurityContextHolder.getContext().getAuthentication() != null)
        {
            ApplicationUser applicationUser = getApplicationUser();
            if (applicationUser != null)
            {
                for (NamespaceAuthorization namespaceAuthorization : applicationUser.getNamespaceAuthorizations())
                {
                    if (namespaceAuthorization.getNamespacePermissions().containsAll(Arrays.asList(permissions)))
                    {
                        namespaces.add(namespaceAuthorization.getNamespace());
                    }
                }
            }
        }
        return namespaces;
    }

    /**
     * Checks the current user's permissions against the given object which may represent a single or multiple namespaces. Allowed types are String or
     * Collection of String.
     *
     * @param object The string or collection of strings which represents the namespace
     * @param permissions The set of permissions the current user must have for the given namespace(s)
     * @param accessDeniedExceptions The list which any access denied exceptions will be gathered into. This list will be empty if no access denied exceptions
     * occur.
     */
    private void checkPermission(Object object, NamespacePermissionEnum[] permissions, List<AccessDeniedException> accessDeniedExceptions)
    {
        /*
         * An infinite recursion is theoretically possible by passing in a collection which contains itself, but given our current usage it may be near
         * impossible to achieve.
         */
        if (object != null)
        {
            if (object instanceof Collection)
            {
                Collection<?> collection = (Collection<?>) object;
                for (Object element : collection)
                {
                    checkPermission(element, permissions, accessDeniedExceptions);
                }
            }
            else if (object instanceof String)
            {
                try
                {
                    checkPermission((String) object, permissions);
                }
                catch (AccessDeniedException accessDeniedException)
                {
                    accessDeniedExceptions.add(accessDeniedException);
                }
            }
            else
            {
                throw new IllegalStateException(
                    String.format("Object must be of type %s or %s. Actual object.class = %s", String.class, Collection.class, object.getClass()));
            }
        }
    }

    /**
     * Gets the ApplicationUser in the current security context. Assumes the user is already authenticated, and the authenticated user is constructed through
     * the application's authentication mechanism.
     *
     * @return The ApplicationUser or null if not authenticated
     */
    private ApplicationUser getApplicationUser()
    {
        Object principal = SecurityContextHolder.getContext().getAuthentication().getPrincipal();
        if (principal != null && principal instanceof SecurityUserWrapper)
        {
            SecurityUserWrapper securityUserWrapper = (SecurityUserWrapper) principal;
            return securityUserWrapper.getApplicationUser();
        }
        return null;
    }

    /**
     * Returns true if there is a current authentication.
     *
     * @return true if authenticated
     */
    private boolean isAuthenticated()
    {
        return SecurityContextHolder.getContext().getAuthentication() != null;
    }
}
