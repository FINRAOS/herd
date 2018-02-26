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
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.BooleanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.core.helper.WildcardHelper;
import org.finra.herd.dao.NamespaceDao;
import org.finra.herd.dao.UserDao;
import org.finra.herd.dao.UserNamespaceAuthorizationDao;
import org.finra.herd.model.api.xml.NamespaceAuthorization;
import org.finra.herd.model.api.xml.NamespaceKey;
import org.finra.herd.model.api.xml.NamespacePermissionEnum;
import org.finra.herd.model.dto.ApplicationUser;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.jpa.UserEntity;
import org.finra.herd.model.jpa.UserNamespaceAuthorizationEntity;

/**
 * A helper class for UserNamespaceAuthorization related code.
 */
@Component
public class UserNamespaceAuthorizationHelper
{
    @Autowired
    private ConfigurationHelper configurationHelper;

    @Autowired
    private NamespaceDao namespaceDao;

    @Autowired
    private UserDao userDao;

    @Autowired
    private UserNamespaceAuthorizationDao userNamespaceAuthorizationDao;

    @Autowired
    private WildcardHelper wildcardHelper;

    /**
     * Builds a set of namespace authorizations per specified user and adds them to the application user.
     *
     * @param applicationUser the application user
     */
    public void buildNamespaceAuthorizations(ApplicationUser applicationUser)
    {
        // Get the user id from the application user.
        String userId = applicationUser.getUserId();

        // Check if user namespace authorization is not enabled or this user is a namespace authorization administrator.
        if (BooleanUtils.isNotTrue(configurationHelper.getBooleanProperty(ConfigurationValue.USER_NAMESPACE_AUTHORIZATION_ENABLED)) ||
            isNamespaceAuthorizationAdmin(userId))
        {
            // Assign all permissions for all namespaces configured in the system.
            applicationUser.setNamespaceAuthorizations(getAllNamespaceAuthorizations());
        }
        else
        {
            // Assign a set of namespace authorizations per specified user.
            Set<NamespaceAuthorization> namespaceAuthorizations = new HashSet<>();
            applicationUser.setNamespaceAuthorizations(namespaceAuthorizations);
            for (UserNamespaceAuthorizationEntity userNamespaceAuthorizationEntity : userNamespaceAuthorizationDao
                .getUserNamespaceAuthorizationsByUserId(userId))
            {
                namespaceAuthorizations.add(toNamespaceAuthorization(userNamespaceAuthorizationEntity));
            }

            // Search authorizations by wildcard token
            for (UserNamespaceAuthorizationEntity wildcardEntity : userNamespaceAuthorizationDao
                .getUserNamespaceAuthorizationsByUserIdStartsWith(WildcardHelper.WILDCARD_TOKEN))
            {
                if (wildcardHelper.matches(userId.toUpperCase(), wildcardEntity.getUserId().toUpperCase()))
                {
                    namespaceAuthorizations.add(toNamespaceAuthorization(wildcardEntity));
                }
            }
        }
    }

    /**
     * Returns a list of namespace authorizations for all namespaces registered in the system and with all permissions enabled.
     *
     * @return namespacePermissions the list of namespace authorizations
     */
    public Set<NamespaceAuthorization> getAllNamespaceAuthorizations()
    {
        Set<NamespaceAuthorization> namespaceAuthorizations = new LinkedHashSet<>();

        List<NamespaceKey> namespaceKeys = namespaceDao.getNamespaces();
        for (NamespaceKey namespaceKey : namespaceKeys)
        {
            NamespaceAuthorization namespaceAuthorization = new NamespaceAuthorization();
            namespaceAuthorizations.add(namespaceAuthorization);
            namespaceAuthorization.setNamespace(namespaceKey.getNamespaceCode());
            namespaceAuthorization.setNamespacePermissions(getAllNamespacePermissions());
        }

        return namespaceAuthorizations;
    }

    /**
     * Returns a list of namespace permissions per specified namespace authorization entity.
     *
     * @param userNamespaceAuthorizationEntity the user namespace authorization entity
     *
     * @return namespacePermissions the list of namespace permissions
     */
    public List<NamespacePermissionEnum> getNamespacePermissions(UserNamespaceAuthorizationEntity userNamespaceAuthorizationEntity)
    {
        List<NamespacePermissionEnum> namespacePermissions = new ArrayList<>();

        if (BooleanUtils.isTrue(userNamespaceAuthorizationEntity.getReadPermission()))
        {
            namespacePermissions.add(NamespacePermissionEnum.READ);
        }
        if (BooleanUtils.isTrue(userNamespaceAuthorizationEntity.getWritePermission()))
        {
            namespacePermissions.add(NamespacePermissionEnum.WRITE);
        }
        if (BooleanUtils.isTrue(userNamespaceAuthorizationEntity.getExecutePermission()))
        {
            namespacePermissions.add(NamespacePermissionEnum.EXECUTE);
        }
        if (BooleanUtils.isTrue(userNamespaceAuthorizationEntity.getGrantPermission()))
        {
            namespacePermissions.add(NamespacePermissionEnum.GRANT);
        }
        if (BooleanUtils.isTrue(userNamespaceAuthorizationEntity.getWriteDescriptiveContentPermission()))
        {
            namespacePermissions.add(NamespacePermissionEnum.WRITE_DESCRIPTIVE_CONTENT);
        }

        return namespacePermissions;
    }

    /**
     * Returns a list of all available namespace permissions.
     *
     * @return namespacePermissions the list of namespace permissions
     */
    private List<NamespacePermissionEnum> getAllNamespacePermissions()
    {
        return Arrays.asList(NamespacePermissionEnum.READ, NamespacePermissionEnum.WRITE, NamespacePermissionEnum.EXECUTE, NamespacePermissionEnum.GRANT,
            NamespacePermissionEnum.WRITE_DESCRIPTIVE_CONTENT);
    }

    /**
     * Converts the given UserNamespaceAuthorizationEntity to NamespaceAuthorization.
     *
     * @param userNamespaceAuthorizationEntity The UserNamespaceAuthorizationEntity
     *
     * @return The NamespaceAuthorization
     */
    private NamespaceAuthorization toNamespaceAuthorization(UserNamespaceAuthorizationEntity userNamespaceAuthorizationEntity)
    {
        NamespaceAuthorization namespaceAuthorization = new NamespaceAuthorization();
        namespaceAuthorization.setNamespace(userNamespaceAuthorizationEntity.getNamespace().getCode());
        namespaceAuthorization.setNamespacePermissions(getNamespacePermissions(userNamespaceAuthorizationEntity));
        return namespaceAuthorization;
    }

    /**
     * Returns true if user is a namespace authorization administrator.
     *
     * @param userId the user id
     *
     * @return true if user is a namespace authorization administrator, false otherwise
     */
    protected boolean isNamespaceAuthorizationAdmin(String userId)
    {
        UserEntity userEntity = userDao.getUserByUserId(userId);
        return userEntity != null ? userEntity.getNamespaceAuthorizationAdmin() : false;
    }
}
