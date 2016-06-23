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

import org.finra.herd.dao.CustomDdlDao;
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.api.xml.CustomDdlKey;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity_;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity_;
import org.finra.herd.model.jpa.CustomDdlEntity;
import org.finra.herd.model.jpa.CustomDdlEntity_;
import org.finra.herd.model.jpa.FileTypeEntity;
import org.finra.herd.model.jpa.FileTypeEntity_;
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.model.jpa.NamespaceEntity_;

@Repository
public class CustomDdlDaoImpl extends AbstractHerdDao implements CustomDdlDao
{
    @Override
    public CustomDdlEntity getCustomDdlByKey(CustomDdlKey customDdlKey)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<CustomDdlEntity> criteria = builder.createQuery(CustomDdlEntity.class);

        // The criteria root is the custom DDL.
        Root<CustomDdlEntity> customDdlEntity = criteria.from(CustomDdlEntity.class);

        // Join to the other tables we can filter on.
        Join<CustomDdlEntity, BusinessObjectFormatEntity> businessObjectFormatEntity = customDdlEntity.join(CustomDdlEntity_.businessObjectFormat);
        Join<BusinessObjectFormatEntity, BusinessObjectDefinitionEntity> businessObjectDefinitionEntity = businessObjectFormatEntity.join(
            BusinessObjectFormatEntity_.businessObjectDefinition);
        Join<BusinessObjectFormatEntity, FileTypeEntity> fileTypeEntity = businessObjectFormatEntity.join(BusinessObjectFormatEntity_.fileType);
        Join<BusinessObjectDefinitionEntity, NamespaceEntity> namespaceEntity = businessObjectDefinitionEntity.join(BusinessObjectDefinitionEntity_.namespace);

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate queryRestriction = builder.equal(builder.upper(namespaceEntity.get(NamespaceEntity_.code)), customDdlKey.getNamespace().toUpperCase());
        queryRestriction = builder.and(queryRestriction, builder.equal(builder.upper(businessObjectDefinitionEntity.get(BusinessObjectDefinitionEntity_.name)),
            customDdlKey.getBusinessObjectDefinitionName().toUpperCase()));
        queryRestriction = builder.and(queryRestriction, builder.equal(builder.upper(businessObjectFormatEntity.get(BusinessObjectFormatEntity_.usage)),
            customDdlKey.getBusinessObjectFormatUsage().toUpperCase()));
        queryRestriction = builder.and(queryRestriction, builder.equal(builder.upper(fileTypeEntity.get(FileTypeEntity_.code)), customDdlKey
            .getBusinessObjectFormatFileType().toUpperCase()));
        queryRestriction = builder.and(queryRestriction, builder.equal(businessObjectFormatEntity.get(BusinessObjectFormatEntity_.businessObjectFormatVersion),
            customDdlKey.getBusinessObjectFormatVersion()));
        queryRestriction = builder.and(queryRestriction, builder.equal(builder.upper(customDdlEntity.get(CustomDdlEntity_.customDdlName)), customDdlKey
            .getCustomDdlName().toUpperCase()));

        // Add select and where clauses.
        criteria.select(customDdlEntity).where(queryRestriction);

        return executeSingleResultQuery(criteria, String.format("Found more than one custom DDL instance with parameters {businessObjectDefinitionName=\"%s\","
            + " businessObjectFormatUsage=\"%s\", businessObjectFormatFileType=\"%s\", businessObjectFormatVersion=\"%d\", customDdlName=\"%s\"}.", customDdlKey
                .getBusinessObjectDefinitionName(), customDdlKey.getBusinessObjectFormatUsage(), customDdlKey.getBusinessObjectFormatFileType(), customDdlKey
                    .getBusinessObjectFormatVersion(), customDdlKey.getCustomDdlName()));
    }

    @Override
    public List<CustomDdlKey> getCustomDdls(BusinessObjectFormatKey businessObjectFormatKey)
    {
        // Create the criteria builder and a tuple style criteria query.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<Tuple> criteria = builder.createTupleQuery();

        // The criteria root is the business object format.
        Root<CustomDdlEntity> customDdlEntity = criteria.from(CustomDdlEntity.class);

        // Join to the other tables we can filter on.
        Join<CustomDdlEntity, BusinessObjectFormatEntity> businessObjectFormatEntity = customDdlEntity.join(CustomDdlEntity_.businessObjectFormat);
        Join<BusinessObjectFormatEntity, BusinessObjectDefinitionEntity> businessObjectDefinitionEntity = businessObjectFormatEntity.join(
            BusinessObjectFormatEntity_.businessObjectDefinition);
        Join<BusinessObjectFormatEntity, FileTypeEntity> fileTypeEntity = businessObjectFormatEntity.join(BusinessObjectFormatEntity_.fileType);
        Join<BusinessObjectDefinitionEntity, NamespaceEntity> namespaceEntity = businessObjectDefinitionEntity.join(BusinessObjectDefinitionEntity_.namespace);

        // Get the columns.
        Path<String> namespaceCodeColumn = namespaceEntity.get(NamespaceEntity_.code);
        Path<String> businessObjectDefinitionNameColumn = businessObjectDefinitionEntity.get(BusinessObjectDefinitionEntity_.name);
        Path<String> businessObjectFormatUsageColumn = businessObjectFormatEntity.get(BusinessObjectFormatEntity_.usage);
        Path<String> fileTypeCodeColumn = fileTypeEntity.get(FileTypeEntity_.code);
        Path<Integer> businessObjectFormatVersionColumn = businessObjectFormatEntity.get(BusinessObjectFormatEntity_.businessObjectFormatVersion);
        Path<String> customDdlNameColumn = customDdlEntity.get(CustomDdlEntity_.customDdlName);

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate queryRestriction = builder.equal(builder.upper(namespaceEntity.get(NamespaceEntity_.code)), businessObjectFormatKey.getNamespace()
            .toUpperCase());
        queryRestriction = builder.and(queryRestriction, builder.equal(builder.upper(businessObjectDefinitionEntity.get(BusinessObjectDefinitionEntity_.name)),
            businessObjectFormatKey.getBusinessObjectDefinitionName().toUpperCase()));
        queryRestriction = builder.and(queryRestriction, builder.equal(builder.upper(businessObjectFormatEntity.get(BusinessObjectFormatEntity_.usage)),
            businessObjectFormatKey.getBusinessObjectFormatUsage().toUpperCase()));
        queryRestriction = builder.and(queryRestriction, builder.equal(builder.upper(fileTypeEntity.get(FileTypeEntity_.code)), businessObjectFormatKey
            .getBusinessObjectFormatFileType().toUpperCase()));
        queryRestriction = builder.and(queryRestriction, builder.equal(businessObjectFormatEntity.get(BusinessObjectFormatEntity_.businessObjectFormatVersion),
            businessObjectFormatKey.getBusinessObjectFormatVersion()));

        // Add the select clause.
        criteria.multiselect(namespaceCodeColumn, businessObjectDefinitionNameColumn, businessObjectFormatUsageColumn, fileTypeCodeColumn,
            businessObjectFormatVersionColumn, customDdlNameColumn);

        // Add the where clause.
        criteria.where(queryRestriction);

        // Add the order by clause.
        criteria.orderBy(builder.asc(customDdlNameColumn));

        // Run the query to get a list of tuples back.
        List<Tuple> tuples = entityManager.createQuery(criteria).getResultList();

        // Populate the "keys" objects from the returned tuples (i.e. 1 tuple for each row).
        List<CustomDdlKey> customDdlKeys = new ArrayList<>();
        for (Tuple tuple : tuples)
        {
            CustomDdlKey customDdlKey = new CustomDdlKey();
            customDdlKeys.add(customDdlKey);
            customDdlKey.setNamespace(tuple.get(namespaceCodeColumn));
            customDdlKey.setBusinessObjectDefinitionName(tuple.get(businessObjectDefinitionNameColumn));
            customDdlKey.setBusinessObjectFormatUsage(tuple.get(businessObjectFormatUsageColumn));
            customDdlKey.setBusinessObjectFormatFileType(tuple.get(fileTypeCodeColumn));
            customDdlKey.setBusinessObjectFormatVersion(tuple.get(businessObjectFormatVersionColumn));
            customDdlKey.setCustomDdlName(tuple.get(customDdlNameColumn));
        }

        return customDdlKeys;
    }
}
