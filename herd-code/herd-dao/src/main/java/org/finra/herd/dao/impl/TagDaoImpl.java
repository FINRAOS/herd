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
import java.util.stream.Collectors;

import javax.persistence.Tuple;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Join;
import javax.persistence.criteria.Path;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;

import org.springframework.stereotype.Repository;

import org.finra.herd.dao.TagDao;
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
    public TagEntity getTagByKey(TagKey tagKey)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<TagEntity> criteria = builder.createQuery(TagEntity.class);

        // The criteria root is the tag code.
        Root<TagEntity> tagEntityRoot = criteria.from(TagEntity.class);

        // Join on the other tables we can filter on.
        Join<TagEntity, TagTypeEntity> tagTypeEntityJoin = tagEntityRoot.join(TagEntity_.tagType);

        // Create the standard restrictions (i.e. the standard where clauses).
        List<Predicate> predicates = new ArrayList<>();
        predicates.add(builder.equal(builder.upper(tagTypeEntityJoin.get(TagTypeEntity_.code)), tagKey.getTagTypeCode().toUpperCase()));
        predicates.add(builder.equal(builder.upper(tagEntityRoot.get(TagEntity_.tagCode)), tagKey.getTagCode().toUpperCase()));

        // Add the clauses for the query.
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

        // The criteria root is the tag code.
        Root<TagEntity> tagEntityRoot = criteria.from(TagEntity.class);

        // Join on the other tables we can filter on.
        Join<TagEntity, TagTypeEntity> tagTypeEntityJoin = tagEntityRoot.join(TagEntity_.tagType);

        // Create the standard restrictions (i.e. the standard where clauses).
        List<Predicate> predicates = new ArrayList<>();
        predicates.add(builder.equal(builder.upper(tagTypeEntityJoin.get(TagTypeEntity_.code)), tagTypeCode.toUpperCase()));
        predicates.add(builder.equal(builder.upper(tagEntityRoot.get(TagEntity_.displayName)), displayName.toUpperCase()));

        // Add the clauses for the query.
        criteria.select(tagEntityRoot).where(builder.and(predicates.toArray(new Predicate[predicates.size()])));

        return executeSingleResultQuery(criteria,
            String.format("Found more than one tag with parameters {tagType=\"%s\", displayName=\"%s\"}.", tagTypeCode, displayName));
    }

    @Override
    public List<TagKey> getTagsByTagType(String tagType)
    {
        // Create the criteria builder and a tuple style criteria query.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<Tuple> criteria = builder.createTupleQuery();

        // The criteria root is the business object definition.
        Root<TagEntity> tagEntityRoot = criteria.from(TagEntity.class);

        // Join to the other tables we can filter on.
        Join<TagEntity, TagTypeEntity> tagTypeEntityJoin = tagEntityRoot.join(TagEntity_.tagType);

        // Get the columns.
        Path<String> tagTypeCodeColumn = tagTypeEntityJoin.get(TagTypeEntity_.code);
        Path<String> tagCodeColumn = tagEntityRoot.get(TagEntity_.tagCode);
        Path<String> displayNameColumn = tagEntityRoot.get(TagEntity_.displayName);

        // Create the standard restrictions (i.e. the standard where clauses).
        List<Predicate> predicates = new ArrayList<>();
        predicates.add(builder.equal(builder.upper(tagTypeEntityJoin.get(TagTypeEntity_.code)), tagType.toUpperCase()));

        // Add the clauses to the query.
        criteria.multiselect(tagTypeCodeColumn, tagCodeColumn).where(builder.and(predicates.toArray(new Predicate[predicates.size()])))
            .orderBy(builder.asc(displayNameColumn));

        // Run the query to get a list of tuples back.
        List<Tuple> tuples = entityManager.createQuery(criteria).getResultList();

        // Populate the "keys" objects from the returned tuples and return (i.e. 1 tuple for each row).
        return tuples.stream().map(tuple -> new TagKey(tuple.get(tagTypeCodeColumn), tuple.get(tagCodeColumn))).collect(Collectors.toList());
    }
}
