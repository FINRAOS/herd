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
package org.finra.herd.dao.impl;

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Join;
import javax.persistence.criteria.Root;

import org.springframework.stereotype.Repository;

import org.finra.herd.dao.NamespaceIamRoleAuthorizationDao;
import org.finra.herd.model.api.xml.NamespaceIamRoleAuthorizationKey;
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.model.jpa.NamespaceEntity_;
import org.finra.herd.model.jpa.NamespaceIamRoleAuthorizationEntity;
import org.finra.herd.model.jpa.NamespaceIamRoleAuthorizationEntity_;

@Repository
public class NamespaceIamRoleAuthorizationDaoImpl extends AbstractHerdDao implements NamespaceIamRoleAuthorizationDao
{
    @PersistenceContext
    private EntityManager entityManager;

    @Override
    public NamespaceIamRoleAuthorizationEntity getNamespaceIamRoleAuthorization(NamespaceIamRoleAuthorizationKey namespaceIamRoleAuthorizationKey)
    {
        CriteriaBuilder criteriaBuilder = entityManager.getCriteriaBuilder();
        CriteriaQuery<NamespaceIamRoleAuthorizationEntity> criteriaQuery = criteriaBuilder.createQuery(NamespaceIamRoleAuthorizationEntity.class);
        Root<NamespaceIamRoleAuthorizationEntity> root = criteriaQuery.from(NamespaceIamRoleAuthorizationEntity.class);
        Join<NamespaceIamRoleAuthorizationEntity, NamespaceEntity> namespaceEntity = root.join(NamespaceIamRoleAuthorizationEntity_.namespace);

        criteriaQuery.where(criteriaBuilder.equal(root.get(NamespaceIamRoleAuthorizationEntity_.namespace), namespaceIamRoleAuthorizationKey.getNamespace()));
        criteriaQuery
            .where(criteriaBuilder.equal(root.get(NamespaceIamRoleAuthorizationEntity_.iamRoleName), namespaceIamRoleAuthorizationKey.getIamRoleName()));

        return executeSingleResultQuery(criteriaQuery, String
            .format("Found more than one namespaceIamRoleAuthorization with namespaceCode=\"%s\" and iamRoleName=\"%s\".",
                namespaceIamRoleAuthorizationKey.getNamespace(), namespaceIamRoleAuthorizationKey.getIamRoleName()));
    }

    @Override
    public List<NamespaceIamRoleAuthorizationEntity> getNamespaceIamRoleAuthorizations(NamespaceEntity namespaceEntity)
    {
        CriteriaBuilder criteriaBuilder = entityManager.getCriteriaBuilder();
        CriteriaQuery<NamespaceIamRoleAuthorizationEntity> query = criteriaBuilder.createQuery(NamespaceIamRoleAuthorizationEntity.class);
        Root<NamespaceIamRoleAuthorizationEntity> root = query.from(NamespaceIamRoleAuthorizationEntity.class);
        Join<NamespaceIamRoleAuthorizationEntity, NamespaceEntity> namespaceJoin = root.join(NamespaceIamRoleAuthorizationEntity_.namespace);

        // If the namespace entity is null then return all namespace IAM role authorizations.
        if (namespaceEntity != null)
        {
            query.where(criteriaBuilder.equal(root.get(NamespaceIamRoleAuthorizationEntity_.namespace), namespaceEntity));
        }

        // Order the namespace IAM role authorization entities by the IAM role name.
        query.orderBy(criteriaBuilder.asc(namespaceJoin.get(NamespaceEntity_.code)), criteriaBuilder.asc(root.get(
            NamespaceIamRoleAuthorizationEntity_.iamRoleName)));

        return entityManager.createQuery(query).getResultList();
    }

    @Override
    public List<NamespaceIamRoleAuthorizationEntity> getNamespaceIamRoleAuthorizationsByIamRoleName(String iamRoleName)
    {
        CriteriaBuilder criteriaBuilder = entityManager.getCriteriaBuilder();
        CriteriaQuery<NamespaceIamRoleAuthorizationEntity> criteriaQuery = criteriaBuilder.createQuery(NamespaceIamRoleAuthorizationEntity.class);
        Root<NamespaceIamRoleAuthorizationEntity> root = criteriaQuery.from(NamespaceIamRoleAuthorizationEntity.class);
        Join<NamespaceIamRoleAuthorizationEntity, NamespaceEntity> namespaceJoin = root.join(NamespaceIamRoleAuthorizationEntity_.namespace);

        criteriaQuery
            .where(criteriaBuilder.equal(root.get(NamespaceIamRoleAuthorizationEntity_.iamRoleName), iamRoleName));

        // Order the namespace IAM role authorization entities by namespace code.
        criteriaQuery.orderBy(criteriaBuilder.asc(namespaceJoin.get(NamespaceEntity_.code)), criteriaBuilder.asc(root.get(
            NamespaceIamRoleAuthorizationEntity_.namespace)));

        return entityManager.createQuery(criteriaQuery).getResultList();
    }
}
