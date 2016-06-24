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
import java.util.Collection;
import java.util.List;

import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Join;
import javax.persistence.criteria.Order;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Repository;

import org.finra.herd.dao.JobDefinitionDao;
import org.finra.herd.model.jpa.JobDefinitionEntity;
import org.finra.herd.model.jpa.JobDefinitionEntity_;
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.model.jpa.NamespaceEntity_;

@Repository
public class JobDefinitionDaoImpl extends AbstractHerdDao implements JobDefinitionDao
{
    @Override
    public JobDefinitionEntity getJobDefinitionByAltKey(String namespace, String jobName)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<JobDefinitionEntity> criteria = builder.createQuery(JobDefinitionEntity.class);

        // The criteria root is the job definition.
        Root<JobDefinitionEntity> jobDefinition = criteria.from(JobDefinitionEntity.class);

        // Join to the other tables we can filter on.
        Join<JobDefinitionEntity, NamespaceEntity> namespaceJoin = jobDefinition.join(JobDefinitionEntity_.namespace);

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate namespaceRestriction = builder.equal(builder.upper(namespaceJoin.get(NamespaceEntity_.code)), namespace.toUpperCase());
        Predicate jobNameRestriction = builder.equal(builder.upper(jobDefinition.get(JobDefinitionEntity_.name)), jobName.toUpperCase());

        criteria.select(jobDefinition).where(builder.and(namespaceRestriction, jobNameRestriction));

        return executeSingleResultQuery(criteria,
            String.format("Found more than one Activiti job definition with parameters {namespace=\"%s\", jobName=\"%s\"}.", namespace, jobName));
    }

    @Override
    public JobDefinitionEntity getJobDefinitionByProcessDefinitionId(String processDefinitionId)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<JobDefinitionEntity> criteria = builder.createQuery(JobDefinitionEntity.class);

        // The criteria root is the job definition.
        Root<JobDefinitionEntity> jobDefinition = criteria.from(JobDefinitionEntity.class);

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate namespaceRestriction = builder.equal(jobDefinition.get(JobDefinitionEntity_.activitiId), processDefinitionId);

        criteria.select(jobDefinition).where(builder.and(namespaceRestriction, namespaceRestriction));

        return executeSingleResultQuery(criteria,
            String.format("Found more than one Activiti job definition with processDefinitionId = \"%s\".", processDefinitionId));
    }

    @Override
    public List<JobDefinitionEntity> getJobDefinitionsByFilter(String namespace, String jobName)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<JobDefinitionEntity> criteria = builder.createQuery(JobDefinitionEntity.class);

        // The criteria root is the job definition.
        Root<JobDefinitionEntity> jobDefinitionEntityRoot = criteria.from(JobDefinitionEntity.class);

        // Join to the other tables we can filter on.
        Join<JobDefinitionEntity, NamespaceEntity> namespaceEntityJoin = jobDefinitionEntityRoot.join(JobDefinitionEntity_.namespace);

        // Create the standard restrictions (i.e. the standard where clauses).
        List<Predicate> predicates = new ArrayList<>();
        if (StringUtils.isNotBlank(namespace))
        {
            predicates.add(builder.equal(builder.upper(namespaceEntityJoin.get(NamespaceEntity_.code)), namespace.toUpperCase()));
        }
        if (StringUtils.isNotBlank(jobName))
        {
            predicates.add(builder.equal(builder.upper(jobDefinitionEntityRoot.get(JobDefinitionEntity_.name)), jobName.toUpperCase()));
        }

        // Order the results by namespace and job name.
        List<Order> orderBy = new ArrayList<>();
        orderBy.add(builder.asc(namespaceEntityJoin.get(NamespaceEntity_.code)));
        orderBy.add(builder.asc(jobDefinitionEntityRoot.get(JobDefinitionEntity_.name)));

        // Add the clauses for the query.
        criteria.select(jobDefinitionEntityRoot).where(builder.and(predicates.toArray(new Predicate[predicates.size()]))).orderBy(orderBy);

        // Execute the query and return the result list.
        return entityManager.createQuery(criteria).getResultList();
    }

    @Override
    public List<JobDefinitionEntity> getJobDefinitionsByFilter(Collection<String> namespaces, String jobName)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<JobDefinitionEntity> criteria = builder.createQuery(JobDefinitionEntity.class);

        // The criteria root is the job definition.
        Root<JobDefinitionEntity> jobDefinitionEntityRoot = criteria.from(JobDefinitionEntity.class);

        // Join to the other tables we can filter on.
        Join<JobDefinitionEntity, NamespaceEntity> namespaceEntityJoin = jobDefinitionEntityRoot.join(JobDefinitionEntity_.namespace);

        // Create the standard restrictions (i.e. the standard where clauses).
        List<Predicate> predicates = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(namespaces))
        {
            predicates.add(namespaceEntityJoin.get(NamespaceEntity_.code).in(namespaces));
        }
        if (StringUtils.isNotBlank(jobName))
        {
            predicates.add(builder.equal(builder.upper(jobDefinitionEntityRoot.get(JobDefinitionEntity_.name)), jobName.toUpperCase()));
        }

        // Order the results by namespace and job name.
        List<Order> orderBy = new ArrayList<>();
        orderBy.add(builder.asc(namespaceEntityJoin.get(NamespaceEntity_.code)));
        orderBy.add(builder.asc(jobDefinitionEntityRoot.get(JobDefinitionEntity_.name)));

        // Add the clauses for the query.
        criteria.select(jobDefinitionEntityRoot).where(builder.and(predicates.toArray(new Predicate[predicates.size()]))).orderBy(orderBy);

        // Execute the query and return the result list.
        return entityManager.createQuery(criteria).getResultList();
    }
}
