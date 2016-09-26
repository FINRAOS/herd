/*
* Copyright 2016 herd contributors
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

import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Order;
import javax.persistence.criteria.Path;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;

import org.springframework.stereotype.Repository;

import org.finra.herd.dao.TagTypeDao;
import org.finra.herd.model.api.xml.TagTypeKey;
import org.finra.herd.model.jpa.TagTypeEntity;
import org.finra.herd.model.jpa.TagTypeEntity_;

@Repository
public class TagTypeDaoImpl extends AbstractHerdDao implements TagTypeDao
{
    @Override
    public TagTypeEntity getTagTypeByKey(TagTypeKey tagTypeKey)
    {
        return getTagTypeByCd(tagTypeKey.getTagTypeCode());
    }

    @Override
    public TagTypeEntity getTagTypeByCd(String tagTypeCode)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<TagTypeEntity> criteria = builder.createQuery(TagTypeEntity.class);

        // The criteria root is the tag type code.
        Root<TagTypeEntity> tagTypeEntity = criteria.from(TagTypeEntity.class);

        // Create the standard restrictions.
        Predicate queryRestriction = builder.equal(builder.upper(tagTypeEntity.get(TagTypeEntity_.typeCode)), tagTypeCode.toUpperCase());

        criteria.select(tagTypeEntity).where(queryRestriction);

        return executeSingleResultQuery(criteria, String.format("Found more than one tag type with tagTypeCode=\"%s\".", tagTypeCode));
    }

    @Override
    public List<TagTypeKey> getTagTypes()
    {
        // Create the criteria builder and a tuple style criteria query.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<String> criteria = builder.createQuery(String.class);

        // The criteria root is the business object definition.
        Root<TagTypeEntity> tagTypeEntity = criteria.from(TagTypeEntity.class);

        // Get the columns.
        Path<String> tagTypeCodeColumn = tagTypeEntity.get(TagTypeEntity_.typeCode);
        Path<String> tagTypeOrderColumn = tagTypeEntity.get(TagTypeEntity_.orderNumber);

        // Add the select clause.
        criteria.select(tagTypeCodeColumn);

        // Add the order by clauses.
        List<Order> orderList = new ArrayList<>();
        orderList.add(builder.asc(tagTypeOrderColumn));
        orderList.add(builder.asc(tagTypeCodeColumn));
        criteria.orderBy(orderList);

        // Run the query to get a list of namespace codes back.
        List<String> tagTypeCodes = entityManager.createQuery(criteria).getResultList();

        // Populate the "keys" objects from the returned namespace codes.
        return tagTypeCodes.stream().map(TagTypeKey::new).collect(Collectors.toList());
    }
}
