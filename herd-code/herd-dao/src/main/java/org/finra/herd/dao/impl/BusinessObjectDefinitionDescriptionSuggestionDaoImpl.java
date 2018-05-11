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

import org.finra.herd.dao.BusinessObjectDefinitionDescriptionSuggestionDao;
import org.finra.herd.model.jpa.BusinessObjectDefinitionDescriptionSuggestionEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionDescriptionSuggestionEntity_;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;

@Repository
public class BusinessObjectDefinitionDescriptionSuggestionDaoImpl extends AbstractHerdDao implements BusinessObjectDefinitionDescriptionSuggestionDao
{
    @Override
    public BusinessObjectDefinitionDescriptionSuggestionEntity getBusinessObjectDefinitionDescriptionSuggestionByBusinessObjectDefinitionEntityAndUserId(
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity, String userId)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<BusinessObjectDefinitionDescriptionSuggestionEntity> criteria =
            builder.createQuery(BusinessObjectDefinitionDescriptionSuggestionEntity.class);

        // The criteria root is the business object definition description suggestion.
        Root<BusinessObjectDefinitionDescriptionSuggestionEntity> businessObjectDefinitionDescriptionSuggestionEntity =
            criteria.from(BusinessObjectDefinitionDescriptionSuggestionEntity.class);

        // Create the standard restrictions (i.e. the standard where clauses).
        List<Predicate> predicates = new ArrayList<>();
        predicates.add(builder
            .equal(businessObjectDefinitionDescriptionSuggestionEntity.get(BusinessObjectDefinitionDescriptionSuggestionEntity_.businessObjectDefinition),
                businessObjectDefinitionEntity));
        predicates.add(builder
            .equal(builder.upper(businessObjectDefinitionDescriptionSuggestionEntity.get(BusinessObjectDefinitionDescriptionSuggestionEntity_.userId)),
                userId.toUpperCase()));

        // Add the clauses for the query.
        criteria.select(businessObjectDefinitionDescriptionSuggestionEntity).where(builder.and(predicates.toArray(new Predicate[predicates.size()])));

        // Get the namespace and business object definition name from the business object definition entity if the business object definition entity is
        // not null otherwise use an empty string.
        String namespace = businessObjectDefinitionEntity == null ? "" : businessObjectDefinitionEntity.getNamespace().getCode();
        String businessObjectDefinitionName = businessObjectDefinitionEntity == null ? "" : businessObjectDefinitionEntity.getName();

        return executeSingleResultQuery(criteria, String.format("Found more than one business object definition description suggestion with parameters " +
            "{namespace=\"%s\", businessObjectDefinitionName=\"%s\", userId=\"%s\"}.", namespace, businessObjectDefinitionName, userId));
    }
}