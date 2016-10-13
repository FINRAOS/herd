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
    public List<TagChild> getTagsByTagType(String tagType, String tagCode)
    {
        List<TagChild> childrenTagKeys = new ArrayList<>();
        // Create the criteria builder and a tuple style criteria query.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<TagEntity> criteria = builder.createQuery(TagEntity.class);

        Root<TagEntity> tagEntityRoot = criteria.from(TagEntity.class);

        // Join to the other tables we can filter on.
        Join<TagEntity, TagTypeEntity> tagTypeEntityJoin = tagEntityRoot.join(TagEntity_.tagType);

        // Get the columns.
        Path<String> displayNameColumn = tagEntityRoot.get(TagEntity_.displayName);
        
        // Create the standard restrictions (i.e. the standard where clauses).
        List<Predicate> predicates = new ArrayList<>();
        predicates.add(builder.equal(builder.upper(tagTypeEntityJoin.get(TagTypeEntity_.code)), tagType.toUpperCase()));

        //no tag code is provided, so return all tag code with no parents, i.e. root
        if (tagCode == null)
        {
            predicates.add(builder.isNull(tagEntityRoot.get(TagEntity_.parentTagEntity)));
        }
        else
        {
            predicates.add(builder.equal(builder.upper(tagEntityRoot.get(TagEntity_.parentTagEntity).get(TagEntity_.tagCode)), tagCode.toUpperCase()));
        }
        
        criteria.select(tagEntityRoot).where(builder.and(predicates.toArray(new Predicate[predicates.size()]))).orderBy(builder.asc(displayNameColumn));
           
        List<TagEntity> tagList = entityManager.createQuery(criteria).getResultList();
        for (TagEntity tag: tagList)
        {           
            boolean hasMoreChildren = tag.getChildrenTagEntities().size() > 0 ? true: false;
            TagChild childTagKey = new TagChild();
            childTagKey.setTagKey(new TagKey(tag.getTagType().getCode(), tag.getTagCode()));
            childTagKey.setHasChildren(hasMoreChildren);        
            childrenTagKeys.add(childTagKey);
        }
       
        return childrenTagKeys;
    }
        

    @Override
    public List<TagChild> getTagsByTagType(String tagTypeCd)
    {
        return getTagsByTagType(tagTypeCd, null);
    }
    
}
