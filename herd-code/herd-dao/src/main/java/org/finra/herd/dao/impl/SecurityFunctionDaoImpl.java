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
import javax.persistence.criteria.Path;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;
import javax.persistence.criteria.Subquery;

import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Repository;

import org.finra.herd.dao.SecurityFunctionDao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.jpa.SecurityFunctionEntity;
import org.finra.herd.model.jpa.SecurityFunctionEntity_;
import org.finra.herd.model.jpa.SecurityRoleEntity;
import org.finra.herd.model.jpa.SecurityRoleEntity_;
import org.finra.herd.model.jpa.SecurityRoleFunctionEntity;
import org.finra.herd.model.jpa.SecurityRoleFunctionEntity_;

@Repository
public class SecurityFunctionDaoImpl extends AbstractHerdDao implements SecurityFunctionDao
{
    @Override
    public SecurityFunctionEntity getSecurityFunctionByName(String securityFunctionName)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<SecurityFunctionEntity> criteria = builder.createQuery(SecurityFunctionEntity.class);

        // The criteria root is the security role function.
        Root<SecurityFunctionEntity> securityFunctionEntity = criteria.from(SecurityFunctionEntity.class);

        // Create the standard restrictions (i.e. the standard where clauses).
        List<Predicate> predicates = new ArrayList<>();
        predicates.add(builder.equal(builder.upper(securityFunctionEntity.get(SecurityFunctionEntity_.code)), securityFunctionName.toUpperCase()));

        // Add the clauses for the query.
        criteria.select(securityFunctionEntity).where(builder.and(predicates.toArray(new Predicate[predicates.size()])));

        return executeSingleResultQuery(criteria,
            String.format("Found more than one security function with parameters {securityFunctionName=\"%s\"}.", securityFunctionName));
    }

    @Override
    @Cacheable(DaoSpringModuleConfig.HERD_CACHE_NAME)
    public List<String> getSecurityFunctions()
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<String> criteria = builder.createQuery(String.class);

        // The criteria root is the security role function.
        Root<SecurityFunctionEntity> securityFunctionEntity = criteria.from(SecurityFunctionEntity.class);

        // Get the columns.
        Path<String> functionCodeColumn = securityFunctionEntity.get(SecurityFunctionEntity_.code);

        // Add the clauses for the query.
        criteria.select(functionCodeColumn).orderBy(builder.asc(functionCodeColumn));

        // Run the query to get a list of functions.
        return entityManager.createQuery(criteria).getResultList();
    }

    @Override
    @Cacheable(DaoSpringModuleConfig.HERD_CACHE_NAME)
    public List<String> getSecurityFunctionsForRole(String roleCd)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<String> criteria = builder.createQuery(String.class);

        // The criteria root is the security role function mapping.
        Root<SecurityRoleFunctionEntity> securityRoleFunctionEntity = criteria.from(SecurityRoleFunctionEntity.class);

        // Join to the other tables we can filter on.
        Join<SecurityRoleFunctionEntity, SecurityRoleEntity> securityRoleEntity = securityRoleFunctionEntity.join(SecurityRoleFunctionEntity_.securityRole);
        Join<SecurityRoleFunctionEntity, SecurityFunctionEntity> securityFunctionEntity =
            securityRoleFunctionEntity.join(SecurityRoleFunctionEntity_.securityFunction);

        // Get the columns.
        Path<String> functionCodeColumn = securityFunctionEntity.get(SecurityFunctionEntity_.code);

        // Add the select clause.
        criteria.select(functionCodeColumn);

        // Add the where clause.
        criteria.where(builder.equal(builder.upper(securityRoleEntity.get(SecurityRoleEntity_.code)), roleCd.toUpperCase()));

        // Add the order by clause.
        criteria.orderBy(builder.asc(functionCodeColumn));

        // Run the query to get a list of functions.
        return entityManager.createQuery(criteria).getResultList();
    }

    @Override
    @Cacheable(DaoSpringModuleConfig.HERD_CACHE_NAME)
    public List<String> getUnrestrictedSecurityFunctions()
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<String> criteria = builder.createQuery(String.class);

        // The criteria root is the security function.
        Root<SecurityFunctionEntity> securityFunctionEntityRoot = criteria.from(SecurityFunctionEntity.class);

        // Build a subquery to eliminate security functions that are mapped to security roles.
        Subquery<SecurityFunctionEntity> subquery = criteria.subquery(SecurityFunctionEntity.class);
        Root<SecurityRoleFunctionEntity> subSecurityRoleFunctionEntityRoot = subquery.from(SecurityRoleFunctionEntity.class);
        subquery.select(subSecurityRoleFunctionEntityRoot.get(SecurityRoleFunctionEntity_.securityFunction))
            .where(builder.equal(subSecurityRoleFunctionEntityRoot.get(SecurityRoleFunctionEntity_.securityFunction), securityFunctionEntityRoot));

        // Get the security function code column.
        Path<String> functionCodeColumn = securityFunctionEntityRoot.get(SecurityFunctionEntity_.code);

        // Add the clauses for the query.
        criteria.select(functionCodeColumn).where(builder.not(builder.exists(subquery))).orderBy(builder.asc(functionCodeColumn));

        // Run the query to get a list of unrestricted security functions.
        return entityManager.createQuery(criteria).getResultList();
    }
}
