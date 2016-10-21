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

import org.finra.herd.dao.BusinessObjectDefinitionTagDao;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionTagKey;
import org.finra.herd.model.api.xml.TagKey;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity_;
import org.finra.herd.model.jpa.BusinessObjectDefinitionTagEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionTagEntity_;
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.model.jpa.NamespaceEntity_;
import org.finra.herd.model.jpa.TagEntity;
import org.finra.herd.model.jpa.TagEntity_;
import org.finra.herd.model.jpa.TagTypeEntity;
import org.finra.herd.model.jpa.TagTypeEntity_;

@Repository
public class BusinessObjectDefinitionTagDaoImpl extends AbstractHerdDao implements BusinessObjectDefinitionTagDao
{
    @Override
    public BusinessObjectDefinitionTagEntity getBusinessObjectDefinitionTagByKey(BusinessObjectDefinitionTagKey businessObjectDefinitionTagKey)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<BusinessObjectDefinitionTagEntity> criteria = builder.createQuery(BusinessObjectDefinitionTagEntity.class);

        // The criteria root is the business object definition tag.
        Root<BusinessObjectDefinitionTagEntity> businessObjectDefinitionTagEntityRoot = criteria.from(BusinessObjectDefinitionTagEntity.class);

        // Join to the other tables we can filter on.
        Join<BusinessObjectDefinitionTagEntity, BusinessObjectDefinitionEntity> businessObjectDefinitionEntityJoin =
            businessObjectDefinitionTagEntityRoot.join(BusinessObjectDefinitionTagEntity_.businessObjectDefinition);
        Join<BusinessObjectDefinitionEntity, NamespaceEntity> namespaceEntityJoin =
            businessObjectDefinitionEntityJoin.join(BusinessObjectDefinitionEntity_.namespace);
        Join<BusinessObjectDefinitionTagEntity, TagEntity> tagEntityJoin = businessObjectDefinitionTagEntityRoot.join(BusinessObjectDefinitionTagEntity_.tag);
        Join<TagEntity, TagTypeEntity> tagTypeEntityJoin = tagEntityJoin.join(TagEntity_.tagType);

        // Create the standard restrictions (i.e. the standard where clauses).
        List<Predicate> predicates = new ArrayList<>();
        predicates.add(builder.equal(builder.upper(namespaceEntityJoin.get(NamespaceEntity_.code)),
            businessObjectDefinitionTagKey.getBusinessObjectDefinitionKey().getNamespace().toUpperCase()));
        predicates.add(builder.equal(builder.upper(businessObjectDefinitionEntityJoin.get(BusinessObjectDefinitionEntity_.name)),
            businessObjectDefinitionTagKey.getBusinessObjectDefinitionKey().getBusinessObjectDefinitionName().toUpperCase()));
        predicates.add(builder
            .equal(builder.upper(tagTypeEntityJoin.get(TagTypeEntity_.code)), businessObjectDefinitionTagKey.getTagKey().getTagTypeCode().toUpperCase()));
        predicates
            .add(builder.equal(builder.upper(tagEntityJoin.get(TagEntity_.tagCode)), businessObjectDefinitionTagKey.getTagKey().getTagCode().toUpperCase()));

        // Add the clauses for the query.
        criteria.select(businessObjectDefinitionTagEntityRoot).where(builder.and(predicates.toArray(new Predicate[predicates.size()])));

        return executeSingleResultQuery(criteria, String.format(
            "Found more than one business object definition tag instance with parameters {namespace=\"%s\", businessObjectDefinitionName=\"%s\"," +
                " tagType=\"%s\", tagCode=\"%s\"}.", businessObjectDefinitionTagKey.getBusinessObjectDefinitionKey().getNamespace(),
            businessObjectDefinitionTagKey.getBusinessObjectDefinitionKey().getBusinessObjectDefinitionName(),
            businessObjectDefinitionTagKey.getTagKey().getTagTypeCode(), businessObjectDefinitionTagKey.getTagKey().getTagCode()));
    }

    @Override
    public BusinessObjectDefinitionTagEntity getBusinessObjectDefinitionTagByParentEntities(BusinessObjectDefinitionEntity businessObjectDefinitionEntity,
        TagEntity tagEntity)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<BusinessObjectDefinitionTagEntity> criteria = builder.createQuery(BusinessObjectDefinitionTagEntity.class);

        // The criteria root is the business object definition tag.
        Root<BusinessObjectDefinitionTagEntity> businessObjectDefinitionTagEntityRoot = criteria.from(BusinessObjectDefinitionTagEntity.class);

        // Create the standard restrictions (i.e. the standard where clauses).
        List<Predicate> predicates = new ArrayList<>();
        predicates.add(builder
            .equal(businessObjectDefinitionTagEntityRoot.get(BusinessObjectDefinitionTagEntity_.businessObjectDefinition), businessObjectDefinitionEntity));
        predicates.add(builder.equal(businessObjectDefinitionTagEntityRoot.get(BusinessObjectDefinitionTagEntity_.tag), tagEntity));

        // Add the clauses for the query.
        criteria.select(businessObjectDefinitionTagEntityRoot).where(builder.and(predicates.toArray(new Predicate[predicates.size()])));

        return executeSingleResultQuery(criteria, String.format(
            "Found more than one business object definition tag instance with parameters {namespace=\"%s\", businessObjectDefinitionName=\"%s\"," +
                " tagType=\"%s\", tagCode=\"%s\"}.", businessObjectDefinitionEntity.getNamespace().getCode(), businessObjectDefinitionEntity.getName(),
            tagEntity.getTagType().getCode(), tagEntity.getTagCode()));
    }

    @Override
    public List<BusinessObjectDefinitionTagKey> getBusinessObjectDefinitionTagsByBusinessObjectDefinitionEntity(
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity)
    {
        // Create the criteria builder and a tuple style criteria query.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<Tuple> criteria = builder.createTupleQuery();

        // The criteria root is the business object definition.
        Root<TagEntity> tagEntityRoot = criteria.from(TagEntity.class);

        // Join to the other tables we can filter on.
        Join<TagEntity, TagTypeEntity> tagTypeEntityJoin = tagEntityRoot.join(TagEntity_.tagType);
        Join<TagEntity, BusinessObjectDefinitionTagEntity> businessObjectDefinitionTagEntityJoin = tagEntityRoot.join(TagEntity_.businessObjectDefinitionTags);
        Join<BusinessObjectDefinitionTagEntity, BusinessObjectDefinitionEntity> businessObjectDefinitionEntityJoin =
            businessObjectDefinitionTagEntityJoin.join(BusinessObjectDefinitionTagEntity_.businessObjectDefinition);

        // Get the columns.
        Path<String> tagTypeCodeColumn = tagTypeEntityJoin.get(TagTypeEntity_.code);
        Path<String> tagCodeColumn = tagEntityRoot.get(TagEntity_.tagCode);
        Path<String> tagDisplayNameColumn = tagEntityRoot.get(TagEntity_.displayName);

        // Create the standard restrictions (i.e. the standard where clauses).
        List<Predicate> predicates = new ArrayList<>();
        predicates.add(builder.equal(businessObjectDefinitionEntityJoin, businessObjectDefinitionEntity));

        // Add the clauses for the query.
        criteria.multiselect(tagTypeCodeColumn, tagCodeColumn).where(builder.and(predicates.toArray(new Predicate[predicates.size()])))
            .orderBy(builder.asc(tagDisplayNameColumn));

        // Run the query to get a list of tuples back.
        List<Tuple> tuples = entityManager.createQuery(criteria).getResultList();

        // Get the business object definition key.
        BusinessObjectDefinitionKey businessObjectDefinitionKey =
            new BusinessObjectDefinitionKey(businessObjectDefinitionEntity.getNamespace().getCode(), businessObjectDefinitionEntity.getName());

        // Populate the "keys" objects from the returned tuples (i.e. 1 tuple for each row).
        List<BusinessObjectDefinitionTagKey> businessObjectDefinitionTagKeys = new ArrayList<>();
        for (Tuple tuple : tuples)
        {
            businessObjectDefinitionTagKeys
                .add(new BusinessObjectDefinitionTagKey(businessObjectDefinitionKey, new TagKey(tuple.get(tagTypeCodeColumn), tuple.get(tagCodeColumn))));
        }

        return businessObjectDefinitionTagKeys;
    }

    @Override
    public List<BusinessObjectDefinitionTagKey> getBusinessObjectDefinitionTagsByTagEntity(TagEntity tagEntity)
    {
        // Create the criteria builder and a tuple style criteria query.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<Tuple> criteria = builder.createTupleQuery();

        // The criteria root is the business object definition.
        Root<BusinessObjectDefinitionEntity> businessObjectDefinitionEntityRoot = criteria.from(BusinessObjectDefinitionEntity.class);

        // Join to the other tables we can filter on.
        Join<BusinessObjectDefinitionEntity, NamespaceEntity> namespaceEntityJoin =
            businessObjectDefinitionEntityRoot.join(BusinessObjectDefinitionEntity_.namespace);
        Join<BusinessObjectDefinitionEntity, BusinessObjectDefinitionTagEntity> businessObjectDefinitionTagEntityJoin =
            businessObjectDefinitionEntityRoot.join(BusinessObjectDefinitionEntity_.businessObjectDefinitionTags);
        Join<BusinessObjectDefinitionTagEntity, TagEntity> tagEntityJoin = businessObjectDefinitionTagEntityJoin.join(BusinessObjectDefinitionTagEntity_.tag);

        // Get the columns.
        Path<String> namespaceCodeColumn = namespaceEntityJoin.get(NamespaceEntity_.code);
        Path<String> businessObjectDefinitionNameColumn = businessObjectDefinitionEntityRoot.get(BusinessObjectDefinitionEntity_.name);
        Path<String> businessObjectDefinitionDisplayNameColumn = businessObjectDefinitionEntityRoot.get(BusinessObjectDefinitionEntity_.displayName);

        // Create the standard restrictions (i.e. the standard where clauses).
        List<Predicate> predicates = new ArrayList<>();
        predicates.add(builder.equal(tagEntityJoin, tagEntity));

        // Add the clauses for the query.
        criteria.multiselect(namespaceCodeColumn, businessObjectDefinitionNameColumn).where(builder.and(predicates.toArray(new Predicate[predicates.size()])))
            .orderBy(builder.asc(businessObjectDefinitionDisplayNameColumn));

        // Run the query to get a list of tuples back.
        List<Tuple> tuples = entityManager.createQuery(criteria).getResultList();

        // Get the business object definition key.
        TagKey tagKey = new TagKey(tagEntity.getTagType().getCode(), tagEntity.getTagCode());

        // Populate the "keys" objects from the returned tuples (i.e. 1 tuple for each row).
        List<BusinessObjectDefinitionTagKey> businessObjectDefinitionTagKeys = new ArrayList<>();
        for (Tuple tuple : tuples)
        {
            businessObjectDefinitionTagKeys.add(new BusinessObjectDefinitionTagKey(
                new BusinessObjectDefinitionKey(tuple.get(namespaceCodeColumn), tuple.get(businessObjectDefinitionNameColumn)), tagKey));
        }

        return businessObjectDefinitionTagKeys;
    }
}
