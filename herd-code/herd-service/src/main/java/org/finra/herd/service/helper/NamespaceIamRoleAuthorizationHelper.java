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

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.NamespaceIamRoleAuthorizationDao;
import org.finra.herd.model.api.xml.NamespaceIamRoleAuthorizationKey;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.model.jpa.NamespaceIamRoleAuthorizationEntity;

/**
 * A helper for NamespaceIamRoleAuthorization
 */
@Component
public class NamespaceIamRoleAuthorizationHelper
{
    @Autowired
    private AlternateKeyHelper alternateKeyHelper;

    @Autowired
    private ConfigurationHelper configurationHelper;

    @Autowired
    private NamespaceDaoHelper namespaceDaoHelper;

    @Autowired
    private NamespaceIamRoleAuthorizationDao namespaceIamRoleAuthorizationDao;

    /**
     * Throws AccessDeniedException if the given namespace is not authorized to access any of the given IAM role names. The IAM role names are case-insensitive.
     * This method does nothing if ConfigurationValue.NAMESPACE_IAM_ROLE_AUTHORIZATION_ENABLED is false.
     *
     * @param namespaceEntity The namespace entity
     * @param requestedIamRoleNames The requested IAM role names
     */
    public void checkPermissions(NamespaceEntity namespaceEntity, String... requestedIamRoleNames)
    {
        checkPermissions(namespaceEntity, Arrays.asList(requestedIamRoleNames));
    }

    /**
     * Throws AccessDeniedException if the given namespace is not authorized to access any of the given IAM role names. The IAM role names are case-insensitive.
     * This method does nothing if ConfigurationValue.NAMESPACE_IAM_ROLE_AUTHORIZATION_ENABLED is false.
     *
     * @param namespaceEntity The namespace entity
     * @param requestedIamRoleNames The collection of requested IAM role names
     */
    public void checkPermissions(NamespaceEntity namespaceEntity, Collection<String> requestedIamRoleNames)
    {
        if (Boolean.TRUE.equals(configurationHelper.getBooleanProperty(ConfigurationValue.NAMESPACE_IAM_ROLE_AUTHORIZATION_ENABLED)))
        {
            // Get the authorized IAM roles as upper case so that we can check in a case-insensitive manner
            Set<String> authorizedIamRoleNamesUpper = new HashSet<>();
            for (NamespaceIamRoleAuthorizationEntity namespaceIamRoleAuthorizationEntity : namespaceIamRoleAuthorizationDao
                .getNamespaceIamRoleAuthorizations(namespaceEntity))
            {
                authorizedIamRoleNamesUpper.add(namespaceIamRoleAuthorizationEntity.getIamRoleName().toUpperCase().trim());
            }

            // Gather unauthorized IAM roles
            Set<String> unauthorizedIamRoles = new TreeSet<>();
            for (String requestedIamRoleName : requestedIamRoleNames)
            {
                // Ignore blank and null IAM roles
                if (StringUtils.isNotBlank(requestedIamRoleName) && !authorizedIamRoleNamesUpper.contains(requestedIamRoleName.toUpperCase().trim()))
                {
                    unauthorizedIamRoles.add(requestedIamRoleName);
                }
            }

            if (!unauthorizedIamRoles.isEmpty())
            {
                throw new AccessDeniedException(
                    String.format("The namespace \"%s\" does not have access to the following IAM roles: %s", namespaceEntity.getCode(), unauthorizedIamRoles));
            }
        }
    }

    /**
     * Creates a new NamespaceIamRoleAuthorizationEntity from the given parameters.
     *
     * @param namespaceIamRoleAuthorizationKey The namespace IAM role authorization key
     *
     * @return The NamespaceIamRoleAuthorizationEntity
     */
    public NamespaceIamRoleAuthorizationEntity createNamespaceIamRoleAuthorizationEntity(
        final NamespaceIamRoleAuthorizationKey namespaceIamRoleAuthorizationKey,
        final String iamRoleDescription)
    {
        NamespaceIamRoleAuthorizationEntity namespaceIamRoleAuthorizationEntity = new NamespaceIamRoleAuthorizationEntity();
        namespaceIamRoleAuthorizationEntity.setNamespace(namespaceDaoHelper.getNamespaceEntity(namespaceIamRoleAuthorizationKey.getNamespace()));
        namespaceIamRoleAuthorizationEntity.setIamRoleName(namespaceIamRoleAuthorizationKey.getIamRoleName());
        if (StringUtils.isNotBlank(iamRoleDescription))
        {
            namespaceIamRoleAuthorizationEntity.setDescription(iamRoleDescription);
        }
        return namespaceIamRoleAuthorizationEntity;
    }

    /**
     * Validates a namespace IAM role authorization key. This method also trims the key parameters.
     *
     * @param namespaceIamRoleAuthorizationKey the namespace IAM role authorization key
     */
    public void validateAndTrimNamespaceIamRoleAuthorizationKey(NamespaceIamRoleAuthorizationKey namespaceIamRoleAuthorizationKey)
    {
        Assert.notNull(namespaceIamRoleAuthorizationKey, "A namespace IAM role authorization key must be specified.");
        namespaceIamRoleAuthorizationKey.setNamespace(
            alternateKeyHelper.validateStringParameter("namespace", namespaceIamRoleAuthorizationKey.getNamespace()));
        namespaceIamRoleAuthorizationKey.setIamRoleName(
            alternateKeyHelper.validateStringParameter("An", "IAM role name", namespaceIamRoleAuthorizationKey.getIamRoleName()));
    }
}
