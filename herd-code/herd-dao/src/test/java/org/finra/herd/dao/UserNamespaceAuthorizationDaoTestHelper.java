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
package org.finra.herd.dao;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.model.api.xml.NamespacePermissionEnum;
import org.finra.herd.model.api.xml.UserNamespaceAuthorizationKey;
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.model.jpa.UserNamespaceAuthorizationEntity;

@Component
public class UserNamespaceAuthorizationDaoTestHelper
{
    @Autowired
    private NamespaceDao namespaceDao;

    @Autowired
    private NamespaceDaoTestHelper namespaceDaoTestHelper;

    @Autowired
    private UserNamespaceAuthorizationDao userNamespaceAuthorizationDao;

    /**
     * Creates and persists a new user namespace authorization entity.
     *
     * @param userNamespaceAuthorizationKey the user namespace authorization key
     * @param namespacePermissions the list of namespace permissions
     *
     * @return the newly created user namespace authorization entity
     */
    public UserNamespaceAuthorizationEntity createUserNamespaceAuthorizationEntity(UserNamespaceAuthorizationKey userNamespaceAuthorizationKey,
        List<NamespacePermissionEnum> namespacePermissions)
    {
        // Create a namespace entity if needed.
        NamespaceEntity namespaceEntity = namespaceDao.getNamespaceByCd(userNamespaceAuthorizationKey.getNamespace());
        if (namespaceEntity == null)
        {
            namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(userNamespaceAuthorizationKey.getNamespace());
        }

        return createUserNamespaceAuthorizationEntity(userNamespaceAuthorizationKey.getUserId(), namespaceEntity, namespacePermissions);
    }

    /**
     * Creates and persists a new user namespace authorization entity.
     *
     * @param userId the user id
     * @param namespaceEntity the namespace entity
     * @param namespacePermissions the list of namespace permissions
     *
     * @return the newly created user namespace authorization entity
     */
    public UserNamespaceAuthorizationEntity createUserNamespaceAuthorizationEntity(String userId, NamespaceEntity namespaceEntity,
        List<NamespacePermissionEnum> namespacePermissions)
    {
        UserNamespaceAuthorizationEntity userNamespaceAuthorizationEntity = new UserNamespaceAuthorizationEntity();

        userNamespaceAuthorizationEntity.setUserId(userId);
        userNamespaceAuthorizationEntity.setNamespace(namespaceEntity);

        userNamespaceAuthorizationEntity.setReadPermission(namespacePermissions.contains(NamespacePermissionEnum.READ));
        userNamespaceAuthorizationEntity.setWritePermission(namespacePermissions.contains(NamespacePermissionEnum.WRITE));
        userNamespaceAuthorizationEntity.setExecutePermission(namespacePermissions.contains(NamespacePermissionEnum.EXECUTE));
        userNamespaceAuthorizationEntity.setGrantPermission(namespacePermissions.contains(NamespacePermissionEnum.GRANT));
        userNamespaceAuthorizationEntity.setWriteDescriptiveContentPermission(namespacePermissions.contains(NamespacePermissionEnum.WRITE_DESCRIPTIVE_CONTENT));
        userNamespaceAuthorizationEntity.setWriteAttributePermission(namespacePermissions.contains(NamespacePermissionEnum.WRITE_ATTRIBUTE));

        return userNamespaceAuthorizationDao.saveAndRefresh(userNamespaceAuthorizationEntity);
    }
}
