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
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;

import org.springframework.stereotype.Repository;

import org.finra.herd.dao.BusinessObjectDataStatusDao;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity_;

@Repository
public class BusinessObjectDataStatusDaoImpl extends AbstractHerdDao implements BusinessObjectDataStatusDao
{
    @Override
    public BusinessObjectDataStatusEntity getBusinessObjectDataStatusByCode(String code)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<BusinessObjectDataStatusEntity> criteria = builder.createQuery(BusinessObjectDataStatusEntity.class);

        // The criteria root is the business object data statuses.
        Root<BusinessObjectDataStatusEntity> businessObjectDataStatus = criteria.from(BusinessObjectDataStatusEntity.class);

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate businessObjectDataStatusCodeRestriction = builder.equal(builder.upper(businessObjectDataStatus.get(BusinessObjectDataStatusEntity_.code)),
            code.toUpperCase());

        criteria.select(businessObjectDataStatus).where(businessObjectDataStatusCodeRestriction);

        return executeSingleResultQuery(criteria, String.format("Found more than one business object data status with code \"%s\".", code));
    }
}
