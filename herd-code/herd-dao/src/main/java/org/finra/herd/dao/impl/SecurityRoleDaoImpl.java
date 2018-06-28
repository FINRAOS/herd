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
import java.util.stream.Collectors;

import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Path;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;

import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Repository;

import org.finra.herd.dao.SecurityRoleDao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.api.xml.SecurityRoleKey;
import org.finra.herd.model.jpa.SecurityRoleEntity;
import org.finra.herd.model.jpa.SecurityRoleEntity_;

@Repository
public class SecurityRoleDaoImpl extends AbstractHerdDao implements SecurityRoleDao
{
    @Override
    @Cacheable(DaoSpringModuleConfig.HERD_CACHE_NAME)
    public List<SecurityRoleEntity> getAllSecurityRoles()
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<SecurityRoleEntity> criteria = builder.createQuery(SecurityRoleEntity.class);

        // The criteria root is the security role
        Root<SecurityRoleEntity> securityRoleEntity = criteria.from(SecurityRoleEntity.class);

        // Create select query
        criteria.select(securityRoleEntity);

        // Get the role code column.
        Path<String> roleCodeColumn = securityRoleEntity.get(SecurityRoleEntity_.code);

        // Set the order by clause
        criteria.orderBy(builder.asc(roleCodeColumn));

        // run the query to get the list of security role entities and return them
        return entityManager.createQuery(criteria).getResultList();
    }

    @Override
    public SecurityRoleEntity getSecurityRoleByName(String securityRoleName)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<SecurityRoleEntity> criteria = builder.createQuery(SecurityRoleEntity.class);

        // The criteria root is the security role.
        Root<SecurityRoleEntity> securityRoleEntity = criteria.from(SecurityRoleEntity.class);

        // Create the standard restrictions (i.e. the standard where clauses).
        List<Predicate> predicates = new ArrayList<>();
        predicates.add(builder.equal(builder.upper(securityRoleEntity.get(SecurityRoleEntity_.code)), securityRoleName.toUpperCase()));

        // Add the clauses for the query.
        criteria.select(securityRoleEntity).where(builder.and(predicates.toArray(new Predicate[predicates.size()])));

        return executeSingleResultQuery(criteria,
            String.format("Found more than one security role with parameters {securityRoleName=\"%s\"}.", securityRoleName));
    }

    @Override
    public List<SecurityRoleKey> getSecurityRoleKeys()
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<String> criteria = builder.createQuery(String.class);

        // The criteria root is the security role.
        Root<SecurityRoleEntity> securityRoleEntity = criteria.from(SecurityRoleEntity.class);

        // Get the columns.
        Path<String> roleCodeColumn = securityRoleEntity.get(SecurityRoleEntity_.code);

        // Add the clauses for the query.
        criteria.select(roleCodeColumn).orderBy(builder.asc(roleCodeColumn));

        // Run the query to get a list of security roles.
        List<String> securityRoles = entityManager.createQuery(criteria).getResultList();

        // Populate the "keys" objects from the security roles and return it.
        return securityRoles.stream().map(SecurityRoleKey::new).collect(Collectors.toList());
    }
}
