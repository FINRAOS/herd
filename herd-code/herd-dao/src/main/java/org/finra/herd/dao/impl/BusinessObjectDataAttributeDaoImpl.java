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
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;

import org.finra.herd.dao.BusinessObjectDataAttributeDao;
import org.finra.herd.model.api.xml.BusinessObjectDataAttributeKey;
import org.finra.herd.model.jpa.BusinessObjectDataAttributeEntity;
import org.finra.herd.model.jpa.BusinessObjectDataAttributeEntity_;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectDataEntity_;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity_;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity_;
import org.finra.herd.model.jpa.FileTypeEntity;
import org.finra.herd.model.jpa.FileTypeEntity_;
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.model.jpa.NamespaceEntity_;

@Repository
public class BusinessObjectDataAttributeDaoImpl extends AbstractHerdDao implements BusinessObjectDataAttributeDao
{
    @Override
    public BusinessObjectDataAttributeEntity getBusinessObjectDataAttributeByKey(BusinessObjectDataAttributeKey businessObjectDataAttributeKey)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<BusinessObjectDataAttributeEntity> criteria = builder.createQuery(BusinessObjectDataAttributeEntity.class);

        // The criteria root is the business object data attribute.
        Root<BusinessObjectDataAttributeEntity> businessObjectDataAttributeEntityRoot = criteria.from(BusinessObjectDataAttributeEntity.class);

        // Join to the other tables we can filter on.
        Join<BusinessObjectDataAttributeEntity, BusinessObjectDataEntity> businessObjectDataEntityJoin =
            businessObjectDataAttributeEntityRoot.join(BusinessObjectDataAttributeEntity_.businessObjectData);
        Join<BusinessObjectDataEntity, BusinessObjectFormatEntity> businessObjectFormatEntityJoin =
            businessObjectDataEntityJoin.join(BusinessObjectDataEntity_.businessObjectFormat);
        Join<BusinessObjectFormatEntity, FileTypeEntity> fileTypeEntityJoin = businessObjectFormatEntityJoin.join(BusinessObjectFormatEntity_.fileType);
        Join<BusinessObjectFormatEntity, BusinessObjectDefinitionEntity> businessObjectDefinitionEntityJoin =
            businessObjectFormatEntityJoin.join(BusinessObjectFormatEntity_.businessObjectDefinition);
        Join<BusinessObjectDefinitionEntity, NamespaceEntity> namespaceEntityJoin =
            businessObjectDefinitionEntityJoin.join(BusinessObjectDefinitionEntity_.namespace);

        // Create the standard restrictions (i.e. the standard where clauses).
        List<Predicate> predicates = new ArrayList<>();
        predicates
            .add(builder.equal(builder.upper(namespaceEntityJoin.get(NamespaceEntity_.code)), businessObjectDataAttributeKey.getNamespace().toUpperCase()));
        predicates.add(builder.equal(builder.upper(businessObjectDefinitionEntityJoin.get(BusinessObjectDefinitionEntity_.name)),
            businessObjectDataAttributeKey.getBusinessObjectDefinitionName().toUpperCase()));
        predicates.add(builder.equal(builder.upper(businessObjectDefinitionEntityJoin.get(BusinessObjectDefinitionEntity_.name)),
            businessObjectDataAttributeKey.getBusinessObjectDefinitionName().toUpperCase()));
        predicates.add(builder.equal(builder.upper(businessObjectFormatEntityJoin.get(BusinessObjectFormatEntity_.usage)),
            businessObjectDataAttributeKey.getBusinessObjectFormatUsage().toUpperCase()));
        predicates.add(builder.equal(builder.upper(fileTypeEntityJoin.get(FileTypeEntity_.code)),
            businessObjectDataAttributeKey.getBusinessObjectFormatFileType().toUpperCase()));
        predicates.add(builder.equal(businessObjectFormatEntityJoin.get(BusinessObjectFormatEntity_.businessObjectFormatVersion),
            businessObjectDataAttributeKey.getBusinessObjectFormatVersion()));
        predicates.add(getQueryRestrictionOnPartitionValues(builder, businessObjectDataEntityJoin, businessObjectDataAttributeKey.getPartitionValue(),
            businessObjectDataAttributeKey.getSubPartitionValues()));
        predicates.add(
            builder.equal(businessObjectDataEntityJoin.get(BusinessObjectDataEntity_.version), businessObjectDataAttributeKey.getBusinessObjectDataVersion()));
        predicates.add(builder.equal(builder.upper(businessObjectDataAttributeEntityRoot.get(BusinessObjectDataAttributeEntity_.name)),
            businessObjectDataAttributeKey.getBusinessObjectDataAttributeName().toUpperCase()));

        // Add the clauses for the query.
        criteria.select(businessObjectDataAttributeEntityRoot).where(builder.and(predicates.toArray(new Predicate[predicates.size()])));

        return executeSingleResultQuery(criteria, String
            .format("Found more than one business object data attribute instance with parameters {namespace=\"%s\", businessObjectDefinitionName=\"%s\"," +
                " businessObjectFormatUsage=\"%s\", businessObjectFormatFileType=\"%s\", businessObjectFormatVersion=\"%d\"," +
                " businessObjectDataPartitionValue=\"%s\", businessObjectDataSubPartitionValues=\"%s\", businessObjectDataVersion=\"%d\"," +
                " businessObjectDataAttributeName=\"%s\"}.", businessObjectDataAttributeKey.getNamespace(),
                businessObjectDataAttributeKey.getBusinessObjectDefinitionName(), businessObjectDataAttributeKey.getBusinessObjectFormatUsage(),
                businessObjectDataAttributeKey.getBusinessObjectFormatFileType(), businessObjectDataAttributeKey.getBusinessObjectFormatVersion(),
                businessObjectDataAttributeKey.getPartitionValue(), CollectionUtils.isEmpty(businessObjectDataAttributeKey.getSubPartitionValues()) ? "" :
                StringUtils.join(businessObjectDataAttributeKey.getSubPartitionValues(), ","), businessObjectDataAttributeKey.getBusinessObjectDataVersion(),
                businessObjectDataAttributeKey.getBusinessObjectDataAttributeName()));
    }
}
