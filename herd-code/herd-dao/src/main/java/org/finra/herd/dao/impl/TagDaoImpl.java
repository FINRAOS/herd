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
import javax.persistence.criteria.Path;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;

import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Repository;

import org.finra.herd.dao.TagDao;
import org.finra.herd.model.api.xml.TagChild;
import org.finra.herd.model.api.xml.TagKey;
import org.finra.herd.model.jpa.TagEntity;
import org.finra.herd.model.jpa.TagEntity_;
import org.finra.herd.model.jpa.TagTypeEntity;
import org.finra.herd.model.jpa.TagTypeEntity_;

/**
 * The tag dao implementation.
 */
@Repository
public class TagDaoImpl extends AbstractHerdDao implements TagDao
{
    @Override
    public List<TagEntity> getChildrenTags(List<TagEntity> parentTagEntities)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<TagEntity> criteria = builder.createQuery(TagEntity.class);

        // The criteria root is the tag entity.
        Root<TagEntity> tagEntityRoot = criteria.from(TagEntity.class);

        // Get the columns.
        Path<String> tagDisplayNameColumn = tagEntityRoot.get(TagEntity_.displayName);

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate predicate = getPredicateForInClause(builder, tagEntityRoot.get(TagEntity_.parentTagEntity), parentTagEntities);

        // Add all clauses to the query.
        criteria.select(tagEntityRoot).where(predicate).orderBy(builder.asc(tagDisplayNameColumn));

        // Run the query to get a list of tag children.
        return entityManager.createQuery(criteria).getResultList();
    }

    @Override
    public TagEntity getTagByKey(TagKey tagKey)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<TagEntity> criteria = builder.createQuery(TagEntity.class);

        // The criteria root is the tag entity.
        Root<TagEntity> tagEntityRoot = criteria.from(TagEntity.class);

        // Join on the other tables we can filter on.
        Join<TagEntity, TagTypeEntity> tagTypeEntityJoin = tagEntityRoot.join(TagEntity_.tagType);

        // Create the standard restrictions (i.e. the standard where clauses).
        List<Predicate> predicates = new ArrayList<>();
        predicates.add(builder.equal(builder.upper(tagTypeEntityJoin.get(TagTypeEntity_.code)), tagKey.getTagTypeCode().toUpperCase()));
        predicates.add(builder.equal(builder.upper(tagEntityRoot.get(TagEntity_.tagCode)), tagKey.getTagCode().toUpperCase()));

        // Add all clauses to the query.
        criteria.select(tagEntityRoot).where(builder.and(predicates.toArray(new Predicate[predicates.size()])));

        return executeSingleResultQuery(criteria,
            String.format("Found more than one tag with parameters {tagType=\"%s\", tagCode=\"%s\"}.", tagKey.getTagTypeCode(), tagKey.getTagCode()));
    }

    @Override
    public TagEntity getTagByTagTypeAndDisplayName(String tagTypeCode, String displayName)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<TagEntity> criteria = builder.createQuery(TagEntity.class);

        // The criteria root is the tag entity.
        Root<TagEntity> tagEntityRoot = criteria.from(TagEntity.class);

        // Join on the other tables we can filter on.
        Join<TagEntity, TagTypeEntity> tagTypeEntityJoin = tagEntityRoot.join(TagEntity_.tagType);

        // Create the standard restrictions (i.e. the standard where clauses).
        List<Predicate> predicates = new ArrayList<>();
        predicates.add(builder.equal(builder.upper(tagTypeEntityJoin.get(TagTypeEntity_.code)), tagTypeCode.toUpperCase()));
        predicates.add(builder.equal(builder.upper(tagEntityRoot.get(TagEntity_.displayName)), displayName.toUpperCase()));

        // Add all clauses to the query.
        criteria.select(tagEntityRoot).where(builder.and(predicates.toArray(new Predicate[predicates.size()])));

        return executeSingleResultQuery(criteria,
            String.format("Found more than one tag with parameters {tagType=\"%s\", displayName=\"%s\"}.", tagTypeCode, displayName));
    }

    @Override
    public List<TagEntity> getTags()
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<TagEntity> criteria = builder.createQuery(TagEntity.class);

        // The criteria root is the tag entity.
        Root<TagEntity> tagEntityRoot = criteria.from(TagEntity.class);

        // Join on the other tables we can filter on.
        Join<TagEntity, TagTypeEntity> tagTypeEntityJoin = tagEntityRoot.join(TagEntity_.tagType);

        // Get the columns.
        Path<String> displayNameColumn = tagEntityRoot.get(TagEntity_.displayName);
        Path<Integer> tagTypeOrderNumberColumn = tagTypeEntityJoin.get(TagTypeEntity_.orderNumber);
        Path<String> tagTypeCodeColumn = tagTypeEntityJoin.get(TagTypeEntity_.code);

        // Add all clauses to the query.
        criteria.select(tagEntityRoot).orderBy(builder.asc(tagTypeOrderNumberColumn), builder.asc(tagTypeCodeColumn), builder.asc(displayNameColumn));

        // Run the query to get the results.
        return entityManager.createQuery(criteria).getResultList();
    }

    @Override
    public List<TagChild> getTagsByTagTypeAndParentTagCode(String tagType, String parentTagCode)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<TagEntity> criteria = builder.createQuery(TagEntity.class);

        // The criteria root is the tag entity.
        Root<TagEntity> tagEntityRoot = criteria.from(TagEntity.class);

        // Join to the other tables we can filter on.
        Join<TagEntity, TagTypeEntity> tagTypeEntityJoin = tagEntityRoot.join(TagEntity_.tagType);

        // Get the columns.
        Path<String> displayNameColumn = tagEntityRoot.get(TagEntity_.displayName);

        // Create the standard restrictions (i.e. the standard where clauses).
        List<Predicate> predicates = new ArrayList<>();
        predicates.add(builder.equal(builder.upper(tagTypeEntityJoin.get(TagTypeEntity_.code)), tagType.toUpperCase()));

        if (parentTagCode == null)
        {
            // Parent tag code is not specified, then return all tags with no parents, i.e. root tags.
            predicates.add(builder.isNull(tagEntityRoot.get(TagEntity_.parentTagEntity)));
        }
        else
        {
            // Add a restriction for the parent tag code.
            predicates.add(builder.equal(builder.upper(tagEntityRoot.get(TagEntity_.parentTagEntity).get(TagEntity_.tagCode)), parentTagCode.toUpperCase()));
        }

        // Add all clauses to the query.
        criteria.select(tagEntityRoot).where(builder.and(predicates.toArray(new Predicate[predicates.size()]))).orderBy(builder.asc(displayNameColumn));

        // Run the query to get a list of tag entities back.
        List<TagEntity> tagEntities = entityManager.createQuery(criteria).getResultList();

        // Populate tag child objects from the returned tag entities.
        List<TagChild> tagChildren = new ArrayList<>();
        for (TagEntity tagEntity : tagEntities)
        {
            boolean hasChildren = !tagEntity.getChildrenTagEntities().isEmpty();
            tagChildren.add(new TagChild(new TagKey(tagEntity.getTagType().getCode(), tagEntity.getTagCode()), hasChildren));
        }

        return tagChildren;
    }

    @Override
    public List<TagEntity> getTagsByTagTypeEntityAndParentTagCode(TagTypeEntity tagTypeEntity, String parentTagCode, Boolean isParentTagNull)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<TagEntity> criteria = builder.createQuery(TagEntity.class);

        // The criteria root is the tag entity.
        Root<TagEntity> tagEntityRoot = criteria.from(TagEntity.class);

        // Get the columns.
        Path<String> displayNameColumn = tagEntityRoot.get(TagEntity_.displayName);

        // Create the standard restrictions (i.e. the standard where clauses).
        List<Predicate> predicates = new ArrayList<>();
        predicates.add(builder.equal(tagEntityRoot.get(TagEntity_.tagType), tagTypeEntity));

        if (StringUtils.isNotBlank(parentTagCode))
        {
            // Return all tags that are immediate children of the specified parent tag.
            predicates.add(builder.equal(builder.upper(tagEntityRoot.get(TagEntity_.parentTagEntity).get(TagEntity_.tagCode)), parentTagCode.toUpperCase()));
        }
        else if (BooleanUtils.isTrue(isParentTagNull))
        {
            // The flag is non-null and true, return all tags with no parents, i.e. root tags.
            predicates.add(builder.isNull(tagEntityRoot.get(TagEntity_.parentTagEntity)));
        }
        else if (BooleanUtils.isFalse(isParentTagNull))
        {
            // The flag is non-null and false, return all tags with parents.
            predicates.add(builder.isNotNull(tagEntityRoot.get(TagEntity_.parentTagEntity)));
        }

        // Add all clauses to the query.
        criteria.select(tagEntityRoot).where(builder.and(predicates.toArray(new Predicate[predicates.size()]))).orderBy(builder.asc(displayNameColumn));

        // Run the query to get the results.
        return entityManager.createQuery(criteria).getResultList();
    }
}
