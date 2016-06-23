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

import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Path;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;

import org.springframework.stereotype.Repository;

import org.finra.herd.dao.OnDemandPriceDao;
import org.finra.herd.model.jpa.OnDemandPriceEntity;
import org.finra.herd.model.jpa.OnDemandPriceEntity_;

@Repository
public class OnDemandPriceDaoImpl extends AbstractHerdDao implements OnDemandPriceDao
{
    /**
     * {@inheritDoc}
     * <p/>
     * This implementation uses JPA criteria, and throws exception when more than 1 on-demand price is found for the specified parameters.
     */
    @Override
    public OnDemandPriceEntity getOnDemandPrice(String region, String instanceType)
    {
        CriteriaBuilder criteriaBuilder = entityManager.getCriteriaBuilder();
        CriteriaQuery<OnDemandPriceEntity> criteriaQuery = criteriaBuilder.createQuery(OnDemandPriceEntity.class);

        Root<OnDemandPriceEntity> onDemandPrice = criteriaQuery.from(OnDemandPriceEntity.class);
        Path<String> regionPath = onDemandPrice.get(OnDemandPriceEntity_.region);
        Path<String> instanceTypePath = onDemandPrice.get(OnDemandPriceEntity_.instanceType);

        Predicate regionEquals = criteriaBuilder.equal(regionPath, region);
        Predicate instanceTypeEquals = criteriaBuilder.equal(instanceTypePath, instanceType);

        criteriaQuery.select(onDemandPrice).where(regionEquals, instanceTypeEquals);

        return executeSingleResultQuery(criteriaQuery, "More than 1 on-demand price found with given region '" + region + "' and instance type '" + instanceType
            + "'.");
    }
}
