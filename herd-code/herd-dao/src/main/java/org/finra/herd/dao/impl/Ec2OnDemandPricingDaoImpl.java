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

import org.springframework.stereotype.Repository;

import org.finra.herd.dao.Ec2OnDemandPricingDao;
import org.finra.herd.model.jpa.Ec2OnDemandPricingEntity;
import org.finra.herd.model.jpa.Ec2OnDemandPricingEntity_;

@Repository
public class Ec2OnDemandPricingDaoImpl extends AbstractHerdDao implements Ec2OnDemandPricingDao
{
    @Override
    public Ec2OnDemandPricingEntity getEc2OnDemandPricing(String regionName, String instanceType)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<Ec2OnDemandPricingEntity> criteria = builder.createQuery(Ec2OnDemandPricingEntity.class);

        // The criteria root is the EC2 on-demand pricing.
        Root<Ec2OnDemandPricingEntity> ec2OnDemandPricingEntityRoot = criteria.from(Ec2OnDemandPricingEntity.class);

        // Create the standard restrictions (i.e. the standard where clauses).
        List<Predicate> predicates = new ArrayList<>();
        predicates.add(builder.equal(ec2OnDemandPricingEntityRoot.get(Ec2OnDemandPricingEntity_.regionName), regionName));
        predicates.add(builder.equal(ec2OnDemandPricingEntityRoot.get(Ec2OnDemandPricingEntity_.instanceType), instanceType));

        // Add all clauses to the query.
        criteria.select(ec2OnDemandPricingEntityRoot).where(builder.and(predicates.toArray(new Predicate[predicates.size()])));

        // Execute the query and return the result.
        return executeSingleResultQuery(criteria, String
            .format("Found more than one EC2 on-demand pricing entity with parameters {regionName=\"%s\", instanceType=\"%s\"}.", regionName, instanceType));
    }

    @Override
    public List<Ec2OnDemandPricingEntity> getEc2OnDemandPricingEntities()
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<Ec2OnDemandPricingEntity> criteria = builder.createQuery(Ec2OnDemandPricingEntity.class);

        // The criteria root is the EC2 on-demand pricing.
        Root<Ec2OnDemandPricingEntity> ec2OnDemandPricingEntityRoot = criteria.from(Ec2OnDemandPricingEntity.class);

        // Get the columns.
        Path<String> regionNameColumn = ec2OnDemandPricingEntityRoot.get(Ec2OnDemandPricingEntity_.regionName);
        Path<String> instanceTypeColumn = ec2OnDemandPricingEntityRoot.get(Ec2OnDemandPricingEntity_.instanceType);

        // Add all clauses to the query.
        criteria.select(ec2OnDemandPricingEntityRoot).orderBy(builder.asc(regionNameColumn), builder.asc(instanceTypeColumn));

        // Execute the query and return the results.
        return entityManager.createQuery(criteria).getResultList();
    }
}
