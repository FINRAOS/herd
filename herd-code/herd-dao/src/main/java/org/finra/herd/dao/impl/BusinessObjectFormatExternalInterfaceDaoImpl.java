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
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;

import org.springframework.stereotype.Repository;

import org.finra.herd.dao.BusinessObjectFormatExternalInterfaceDao;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatExternalInterfaceEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatExternalInterfaceEntity_;
import org.finra.herd.model.jpa.ExternalInterfaceEntity;

@Repository
public class BusinessObjectFormatExternalInterfaceDaoImpl extends AbstractHerdDao implements BusinessObjectFormatExternalInterfaceDao
{
    @Override
    public BusinessObjectFormatExternalInterfaceEntity getBusinessObjectFormatExternalInterfaceByBusinessObjectFormatAndExternalInterface(
        BusinessObjectFormatEntity businessObjectFormatEntity, ExternalInterfaceEntity externalInterfaceEntity)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<BusinessObjectFormatExternalInterfaceEntity> criteria = builder.createQuery(BusinessObjectFormatExternalInterfaceEntity.class);

        // The criteria root is the business object format to external interface mapping.
        Root<BusinessObjectFormatExternalInterfaceEntity> businessObjectFormatExternalInterfaceEntityRoot =
            criteria.from(BusinessObjectFormatExternalInterfaceEntity.class);

        // Create the standard restrictions (i.e. the standard where clauses).
        List<Predicate> predicates = new ArrayList<>();
        predicates.add(builder.equal(businessObjectFormatExternalInterfaceEntityRoot.get(BusinessObjectFormatExternalInterfaceEntity_.businessObjectFormatId),
            businessObjectFormatEntity.getId()));
        predicates.add(builder.equal(businessObjectFormatExternalInterfaceEntityRoot.get(BusinessObjectFormatExternalInterfaceEntity_.externalInterfaceName),
            externalInterfaceEntity.getCode()));

        // Add all clauses to the query.
        criteria.select(businessObjectFormatExternalInterfaceEntityRoot).where(builder.and(predicates.toArray(new Predicate[predicates.size()])));

        // Execute the query and return the result.
        return executeSingleResultQuery(criteria, String.format("Found more than one business object format to external interface mapping with parameters " +
                "{namespace=\"%s\", businessObjectDefinitionName=\"%s\", businessObjectFormatUsage=\"%s\", businessObjectFormatFileType=\"%s\", " +
                "externalInterfaceName=\"%s\"}.", businessObjectFormatEntity.getBusinessObjectDefinition().getNamespace().getCode(),
            businessObjectFormatEntity.getBusinessObjectDefinition().getName(), businessObjectFormatEntity.getUsage(),
            businessObjectFormatEntity.getFileType().getCode(), externalInterfaceEntity.getCode()));
    }
}
