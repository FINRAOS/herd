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

import org.finra.herd.model.api.xml.NamespaceIamRoleAuthorizationKey;
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.model.jpa.NamespaceIamRoleAuthorizationEntity;

/**
 * The DAO for NamespaceIamRoleAuthorizations
 */
public interface NamespaceIamRoleAuthorizationDao extends BaseJpaDao
{
    /**
     * Get a NamespaceIamRoleAuthorizationEntity by the namespaceIamRoleAuthorizationKey.
     *
     * @param namespaceIamRoleAuthorizationKey The namespace IAM role authorization key consisting of the namespace code and the IAM role name
     *
     * @return NamespaceIamRoleAuthorization
     */
    NamespaceIamRoleAuthorizationEntity getNamespaceIamRoleAuthorization(NamespaceIamRoleAuthorizationKey namespaceIamRoleAuthorizationKey);

    /**
     * Get a list of NamespaceIamRoleAuthorizationEntity by a namespace. If namespace is null, all authorizations are returned.
     *
     * @param namespaceEntity The namespace entity
     *
     * @return List of NamespaceIamRoleAuthorizations
     */
    List<NamespaceIamRoleAuthorizationEntity> getNamespaceIamRoleAuthorizations(NamespaceEntity namespaceEntity);

    /**
     * Get a list of NamespaceIamRoleAuthorizationEntity by an IAM role name.
     *
     * @param iamRoleName The IAM role name
     *
     * @return List of NamespaceIamRoleAuthorizations
     */
    List<NamespaceIamRoleAuthorizationEntity> getNamespaceIamRoleAuthorizationsByIamRoleName(String iamRoleName);
}
