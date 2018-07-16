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

import java.util.ArrayList;
import java.util.List;

import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Join;
import javax.persistence.criteria.Order;
import javax.persistence.criteria.Path;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;

import org.springframework.stereotype.Repository;

import org.finra.herd.dao.UserNamespaceAuthorizationDao;
import org.finra.herd.model.api.xml.UserNamespaceAuthorizationKey;
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.model.jpa.NamespaceEntity_;
import org.finra.herd.model.jpa.UserNamespaceAuthorizationEntity;
import org.finra.herd.model.jpa.UserNamespaceAuthorizationEntity_;

@Repository
public class UserNamespaceAuthorizationDaoImpl extends AbstractHerdDao implements UserNamespaceAuthorizationDao
{
    @Override
    public List<String> getUserIdsWithWriteOrWriteDescriptiveContentPermissionsByNamespace(NamespaceEntity namespaceEntity)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<String> criteria = builder.createQuery(String.class);

        // The criteria root is the user namespace authorization.
        Root<UserNamespaceAuthorizationEntity> userNamespaceAuthorizationEntityRoot = criteria.from(UserNamespaceAuthorizationEntity.class);

        // Get the user id column.
        Path<String> userIdColumn = userNamespaceAuthorizationEntityRoot.get(UserNamespaceAuthorizationEntity_.userId);

        // Create the standard restrictions (i.e. the standard where clauses).
        List<Predicate> predicates = new ArrayList<>();
        predicates.add(builder.equal(userNamespaceAuthorizationEntityRoot.get(UserNamespaceAuthorizationEntity_.namespace), namespaceEntity));
        predicates.add(builder.or(builder.isTrue(userNamespaceAuthorizationEntityRoot.get(UserNamespaceAuthorizationEntity_.writePermission)),
            builder.isTrue(userNamespaceAuthorizationEntityRoot.get(UserNamespaceAuthorizationEntity_.writeDescriptiveContentPermission))));

        // Order by user id.
        Order orderBy = builder.asc(userNamespaceAuthorizationEntityRoot.get(UserNamespaceAuthorizationEntity_.userId));

        // Add all clauses to the query.
        criteria.select(userIdColumn).where(builder.and(predicates.toArray(new Predicate[predicates.size()]))).orderBy(orderBy);

        // Execute the query and return the result list.
        return entityManager.createQuery(criteria).getResultList();
    }

    @Override
    public UserNamespaceAuthorizationEntity getUserNamespaceAuthorizationByKey(UserNamespaceAuthorizationKey userNamespaceAuthorizationKey)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<UserNamespaceAuthorizationEntity> criteria = builder.createQuery(UserNamespaceAuthorizationEntity.class);

        // The criteria root is the user namespace authorization.
        Root<UserNamespaceAuthorizationEntity> userNamespaceAuthorizationEntity = criteria.from(UserNamespaceAuthorizationEntity.class);

        // Join to the other tables we can filter on.
        Join<UserNamespaceAuthorizationEntity, NamespaceEntity> namespaceEntity =
            userNamespaceAuthorizationEntity.join(UserNamespaceAuthorizationEntity_.namespace);

        // Create the standard restrictions (i.e. the standard where clauses).
        List<Predicate> predicates = new ArrayList<>();
        predicates.add(builder.equal(builder.upper(namespaceEntity.get(NamespaceEntity_.code)), userNamespaceAuthorizationKey.getNamespace().toUpperCase()));
        predicates.add(builder.equal(builder.upper(userNamespaceAuthorizationEntity.get(UserNamespaceAuthorizationEntity_.userId)),
            userNamespaceAuthorizationKey.getUserId().toUpperCase()));

        // Add the clauses for the query.
        criteria.select(userNamespaceAuthorizationEntity).where(builder.and(predicates.toArray(new Predicate[predicates.size()])));

        return executeSingleResultQuery(criteria, String
            .format("Found more than one user namespace authorization with parameters {userId=\"%s\", namespace=\"%s\"}.",
                userNamespaceAuthorizationKey.getUserId(), userNamespaceAuthorizationKey.getNamespace()));
    }

    @Override
    public List<UserNamespaceAuthorizationEntity> getUserNamespaceAuthorizationsByUserId(String userId)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<UserNamespaceAuthorizationEntity> criteria = builder.createQuery(UserNamespaceAuthorizationEntity.class);

        // The criteria root is the user namespace authorization.
        Root<UserNamespaceAuthorizationEntity> userNamespaceAuthorizationEntity = criteria.from(UserNamespaceAuthorizationEntity.class);

        // Join to the other tables we can filter on.
        Join<UserNamespaceAuthorizationEntity, NamespaceEntity> namespaceEntity =
            userNamespaceAuthorizationEntity.join(UserNamespaceAuthorizationEntity_.namespace);

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate queryRestriction =
            builder.equal(builder.upper(userNamespaceAuthorizationEntity.get(UserNamespaceAuthorizationEntity_.userId)), userId.toUpperCase());

        // Order by namespace code.
        Order orderBy = builder.asc(namespaceEntity.get(NamespaceEntity_.code));

        // Add all clauses for the query.
        criteria.select(userNamespaceAuthorizationEntity).where(queryRestriction).orderBy(orderBy);

        // Execute the query and return the result list.
        return entityManager.createQuery(criteria).getResultList();
    }

    @Override
    public List<UserNamespaceAuthorizationEntity> getUserNamespaceAuthorizationsByUserIdStartsWith(String userIdStartsWith)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<UserNamespaceAuthorizationEntity> criteria = builder.createQuery(UserNamespaceAuthorizationEntity.class);

        // The criteria root is the user namespace authorization.
        Root<UserNamespaceAuthorizationEntity> userNamespaceAuthorizationEntity = criteria.from(UserNamespaceAuthorizationEntity.class);

        // Join to the other tables we can filter on.
        Join<UserNamespaceAuthorizationEntity, NamespaceEntity> namespaceEntity =
            userNamespaceAuthorizationEntity.join(UserNamespaceAuthorizationEntity_.namespace);

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate queryRestriction =
            builder.like(builder.upper(userNamespaceAuthorizationEntity.get(UserNamespaceAuthorizationEntity_.userId)), userIdStartsWith.toUpperCase() + '%');

        // Add all clauses for the query.
        criteria.select(userNamespaceAuthorizationEntity).where(queryRestriction)
            .orderBy(builder.asc(userNamespaceAuthorizationEntity.get(UserNamespaceAuthorizationEntity_.userId)),
                builder.asc(namespaceEntity.get(NamespaceEntity_.code)));

        // Execute the query and return the result list.
        return entityManager.createQuery(criteria).getResultList();
    }

    @Override
    public List<UserNamespaceAuthorizationEntity> getUserNamespaceAuthorizationsByNamespace(String namespace)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<UserNamespaceAuthorizationEntity> criteria = builder.createQuery(UserNamespaceAuthorizationEntity.class);

        // The criteria root is the user namespace authorization.
        Root<UserNamespaceAuthorizationEntity> userNamespaceAuthorizationEntity = criteria.from(UserNamespaceAuthorizationEntity.class);

        // Join to the other tables we can filter on.
        Join<UserNamespaceAuthorizationEntity, NamespaceEntity> namespaceEntity =
            userNamespaceAuthorizationEntity.join(UserNamespaceAuthorizationEntity_.namespace);

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate queryRestriction = builder.equal(builder.upper(namespaceEntity.get(NamespaceEntity_.code)), namespace.toUpperCase());

        // Order by user id.
        Order orderBy = builder.asc(userNamespaceAuthorizationEntity.get(UserNamespaceAuthorizationEntity_.userId));

        // Add all clauses for the query.
        criteria.select(userNamespaceAuthorizationEntity).where(queryRestriction).orderBy(orderBy);

        // Execute the query and return the result list.
        return entityManager.createQuery(criteria).getResultList();
    }
}
