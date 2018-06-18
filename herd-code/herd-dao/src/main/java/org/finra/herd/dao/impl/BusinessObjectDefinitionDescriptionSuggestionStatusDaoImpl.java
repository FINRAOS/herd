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

import org.finra.herd.dao.BusinessObjectDefinitionDescriptionSuggestionStatusDao;
import org.finra.herd.model.jpa.BusinessObjectDefinitionDescriptionSuggestionStatusEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionDescriptionSuggestionStatusEntity_;

@Repository
public class BusinessObjectDefinitionDescriptionSuggestionStatusDaoImpl extends AbstractHerdDao
    implements BusinessObjectDefinitionDescriptionSuggestionStatusDao
{
    @Override
    public BusinessObjectDefinitionDescriptionSuggestionStatusEntity getBusinessObjectDefinitionDescriptionSuggestionStatusByCode(String code)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<BusinessObjectDefinitionDescriptionSuggestionStatusEntity> criteria =
            builder.createQuery(BusinessObjectDefinitionDescriptionSuggestionStatusEntity.class);

        // The criteria root is the search index status.
        Root<BusinessObjectDefinitionDescriptionSuggestionStatusEntity> businessObjectDefinitionDescriptionSuggestionEntityRoot =
            criteria.from(BusinessObjectDefinitionDescriptionSuggestionStatusEntity.class);

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate predicate = builder
            .equal(builder.upper(businessObjectDefinitionDescriptionSuggestionEntityRoot.get(BusinessObjectDefinitionDescriptionSuggestionStatusEntity_.code)),
                code.toUpperCase());

        // Add all clauses to the query.
        criteria.select(businessObjectDefinitionDescriptionSuggestionEntityRoot).where(predicate);

        // Execute the query and return the result.
        return executeSingleResultQuery(criteria,
            String.format("Found more than one business object definition description suggestion status with code \"%s\".", code));
    }
}
