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

import javax.persistence.Tuple;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Join;
import javax.persistence.criteria.Path;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;

import org.springframework.stereotype.Repository;

import org.finra.herd.dao.SecurityRoleFunctionDao;
import org.finra.herd.model.api.xml.SecurityRoleFunctionKey;
import org.finra.herd.model.jpa.SecurityFunctionEntity;
import org.finra.herd.model.jpa.SecurityFunctionEntity_;
import org.finra.herd.model.jpa.SecurityRoleEntity;
import org.finra.herd.model.jpa.SecurityRoleEntity_;
import org.finra.herd.model.jpa.SecurityRoleFunctionEntity;
import org.finra.herd.model.jpa.SecurityRoleFunctionEntity_;

@Repository
public class SecurityRoleFunctionDaoImpl extends AbstractHerdDao implements SecurityRoleFunctionDao
{
    @Override
    public SecurityRoleFunctionEntity getSecurityRoleFunctionByKey(SecurityRoleFunctionKey securityRoleFunctionKey)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<SecurityRoleFunctionEntity> criteria = builder.createQuery(SecurityRoleFunctionEntity.class);

        // The criteria root is the security role to function mapping.
        Root<SecurityRoleFunctionEntity> securityRoleFunctionEntityRoot = criteria.from(SecurityRoleFunctionEntity.class);

        // Join on the other tables.
        Join<SecurityRoleFunctionEntity, SecurityRoleEntity> securityRoleEntityJoin =
            securityRoleFunctionEntityRoot.join(SecurityRoleFunctionEntity_.securityRole);
        Join<SecurityRoleFunctionEntity, SecurityFunctionEntity> securityFunctionEntityJoin =
            securityRoleFunctionEntityRoot.join(SecurityRoleFunctionEntity_.securityFunction);

        // Create the standard restrictions (i.e. the standard where clauses).
        List<Predicate> predicates = new ArrayList<>();
        predicates.add(
            builder.equal(builder.upper(securityRoleEntityJoin.get(SecurityRoleEntity_.code)), securityRoleFunctionKey.getSecurityRoleName().toUpperCase()));
        predicates.add(builder.equal(builder.upper(securityFunctionEntityJoin.get(SecurityFunctionEntity_.code)),
            securityRoleFunctionKey.getSecurityFunctionName().toUpperCase()));

        // Add all clauses to the query.
        criteria.select(securityRoleFunctionEntityRoot).where(builder.and(predicates.toArray(new Predicate[predicates.size()])));

        // Execute the query and return the result.
        return executeSingleResultQuery(criteria, String
            .format("Found more than one security role to function mapping with parameters {securityRoleName=\"%s\", securityFunctionName=\"%s\"}.",
                securityRoleFunctionKey.getSecurityRoleName(), securityRoleFunctionKey.getSecurityFunctionName()));
    }

    @Override
    public List<SecurityRoleFunctionKey> getSecurityRoleFunctionKeys()
    {
        // Create the criteria builder and a tuple style criteria query.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<Tuple> criteria = builder.createTupleQuery();

        // The criteria root is the security role to function mapping.
        Root<SecurityRoleFunctionEntity> securityRoleFunctionEntityRoot = criteria.from(SecurityRoleFunctionEntity.class);

        // Join to the other tables.
        Join<SecurityRoleFunctionEntity, SecurityRoleEntity> securityRoleEntityJoin =
            securityRoleFunctionEntityRoot.join(SecurityRoleFunctionEntity_.securityRole);
        Join<SecurityRoleFunctionEntity, SecurityFunctionEntity> securityFunctionEntityJoin =
            securityRoleFunctionEntityRoot.join(SecurityRoleFunctionEntity_.securityFunction);

        // Get the columns.
        Path<String> securityRoleNameColumn = securityRoleEntityJoin.get(SecurityRoleEntity_.code);
        Path<String> securityFunctionNameColumn = securityFunctionEntityJoin.get(SecurityFunctionEntity_.code);

        // Add all clauses to the query.
        criteria.multiselect(securityRoleNameColumn, securityFunctionNameColumn)
            .orderBy(builder.asc(securityRoleNameColumn), builder.asc(securityFunctionNameColumn));

        // Run the query to get a list of tuples back.
        List<Tuple> tuples = entityManager.createQuery(criteria).getResultList();

        // Populate the "keys" objects from the returned tuples (i.e. 1 tuple for each row).
        List<SecurityRoleFunctionKey> securityRoleFunctionKeys = new ArrayList<>();
        for (Tuple tuple : tuples)
        {
            securityRoleFunctionKeys.add(new SecurityRoleFunctionKey(tuple.get(securityRoleNameColumn), tuple.get(securityFunctionNameColumn)));
        }

        // Return the result list.
        return securityRoleFunctionKeys;
    }

    @Override
    public List<SecurityRoleFunctionKey> getSecurityRoleFunctionKeysBySecurityFunction(String securityFunctionName)
    {
        // Create the criteria builder and a tuple style criteria query.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<Tuple> criteria = builder.createTupleQuery();

        // The criteria root is the security role to function mapping.
        Root<SecurityRoleFunctionEntity> securityRoleFunctionEntityRoot = criteria.from(SecurityRoleFunctionEntity.class);

        // Join to the other tables.
        Join<SecurityRoleFunctionEntity, SecurityRoleEntity> securityRoleEntityJoin =
            securityRoleFunctionEntityRoot.join(SecurityRoleFunctionEntity_.securityRole);
        Join<SecurityRoleFunctionEntity, SecurityFunctionEntity> securityFunctionEntityJoin =
            securityRoleFunctionEntityRoot.join(SecurityRoleFunctionEntity_.securityFunction);

        // Get the columns.
        Path<String> securityRoleNameColumn = securityRoleEntityJoin.get(SecurityRoleEntity_.code);
        Path<String> securityFunctionNameColumn = securityFunctionEntityJoin.get(SecurityFunctionEntity_.code);

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate queryRestriction =
            builder.equal(builder.upper(securityFunctionEntityJoin.get(SecurityFunctionEntity_.code)), securityFunctionName.toUpperCase());

        // Add all clauses to the query.
        criteria.multiselect(securityRoleNameColumn, securityFunctionNameColumn).where(queryRestriction).orderBy(builder.asc(securityRoleNameColumn));

        // Run the query to get a list of tuples back.
        List<Tuple> tuples = entityManager.createQuery(criteria).getResultList();

        // Populate the "keys" objects from the returned tuples (i.e. 1 tuple for each row).
        List<SecurityRoleFunctionKey> securityRoleFunctionKeys = new ArrayList<>();
        for (Tuple tuple : tuples)
        {
            securityRoleFunctionKeys.add(new SecurityRoleFunctionKey(tuple.get(securityRoleNameColumn), tuple.get(securityFunctionNameColumn)));
        }

        // Return the result list.
        return securityRoleFunctionKeys;
    }

    @Override
    public List<SecurityRoleFunctionKey> getSecurityRoleFunctionKeysBySecurityRole(String securityRoleName)
    {
        // Create the criteria builder and a tuple style criteria query.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<Tuple> criteria = builder.createTupleQuery();

        // The criteria root is the security role to function mapping.
        Root<SecurityRoleFunctionEntity> securityRoleFunctionEntityRoot = criteria.from(SecurityRoleFunctionEntity.class);

        // Join to the other tables.
        Join<SecurityRoleFunctionEntity, SecurityRoleEntity> securityRoleEntityJoin =
            securityRoleFunctionEntityRoot.join(SecurityRoleFunctionEntity_.securityRole);
        Join<SecurityRoleFunctionEntity, SecurityFunctionEntity> securityFunctionEntityJoin =
            securityRoleFunctionEntityRoot.join(SecurityRoleFunctionEntity_.securityFunction);

        // Get the columns.
        Path<String> securityRoleNameColumn = securityRoleEntityJoin.get(SecurityRoleEntity_.code);
        Path<String> securityFunctionNameColumn = securityFunctionEntityJoin.get(SecurityFunctionEntity_.code);

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate queryRestriction = builder.equal(builder.upper(securityRoleEntityJoin.get(SecurityRoleEntity_.code)), securityRoleName.toUpperCase());

        // Add all clauses to the query.
        criteria.multiselect(securityRoleNameColumn, securityFunctionNameColumn).where(queryRestriction).orderBy(builder.asc(securityFunctionNameColumn));

        // Run the query to get a list of tuples back.
        List<Tuple> tuples = entityManager.createQuery(criteria).getResultList();

        // Populate the "keys" objects from the returned tuples (i.e. 1 tuple for each row).
        List<SecurityRoleFunctionKey> securityRoleFunctionKeys = new ArrayList<>();
        for (Tuple tuple : tuples)
        {
            securityRoleFunctionKeys.add(new SecurityRoleFunctionKey(tuple.get(securityRoleNameColumn), tuple.get(securityFunctionNameColumn)));
        }

        // Return the result list.
        return securityRoleFunctionKeys;
    }
}
