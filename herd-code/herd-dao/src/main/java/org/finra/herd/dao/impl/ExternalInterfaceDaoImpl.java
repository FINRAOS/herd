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
import javax.persistence.criteria.Path;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;

import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Repository;

import org.finra.herd.dao.ExternalInterfaceDao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.jpa.ExternalInterfaceEntity;
import org.finra.herd.model.jpa.ExternalInterfaceEntity_;

@Repository
public class ExternalInterfaceDaoImpl extends AbstractHerdDao implements ExternalInterfaceDao
{
    @Override
    public ExternalInterfaceEntity getExternalInterfaceByName(String externalInterfaceName)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<ExternalInterfaceEntity> criteria = builder.createQuery(ExternalInterfaceEntity.class);

        // The criteria root is the external interface.
        Root<ExternalInterfaceEntity> externalInterfaceEntity = criteria.from(ExternalInterfaceEntity.class);

        // Create the standard restrictions (i.e. the standard where clauses).
        List<Predicate> predicates = new ArrayList<>();
        predicates.add(builder.equal(builder.upper(externalInterfaceEntity.get(ExternalInterfaceEntity_.code)), externalInterfaceName.toUpperCase()));

        // Add the clauses for the query.
        criteria.select(externalInterfaceEntity).where(builder.and(predicates.toArray(new Predicate[predicates.size()])));

        return executeSingleResultQuery(criteria,
            String.format("Found more than one external interface with parameters {externalInterfaceName=\"%s\"}.", externalInterfaceName));
    }

    @Override
    @Cacheable(DaoSpringModuleConfig.HERD_CACHE_NAME)
    public List<String> getExternalInterfaces()
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<String> criteria = builder.createQuery(String.class);

        // The criteria root is the external interface.
        Root<ExternalInterfaceEntity> externalInterfaceEntity = criteria.from(ExternalInterfaceEntity.class);

        // Get the columns.
        Path<String> externalInterfaceCodeColumn = externalInterfaceEntity.get(ExternalInterfaceEntity_.code);

        // Add the clauses for the query.
        criteria.select(externalInterfaceCodeColumn).orderBy(builder.asc(externalInterfaceCodeColumn));

        // Run the query to get a list of external interfaces.
        return entityManager.createQuery(criteria).getResultList();
    }
}
