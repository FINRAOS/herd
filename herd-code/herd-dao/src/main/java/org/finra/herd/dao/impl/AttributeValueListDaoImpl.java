package org.finra.herd.dao.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Join;
import javax.persistence.criteria.Order;
import javax.persistence.criteria.Path;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;

import org.springframework.stereotype.Repository;

import org.finra.herd.dao.AttributeValueListDao;
import org.finra.herd.model.api.xml.AttributeValueList;
import org.finra.herd.model.api.xml.AttributeValueListKey;
import org.finra.herd.model.api.xml.AttributeValueListKeys;
import org.finra.herd.model.jpa.AttributeValueListEntity;
import org.finra.herd.model.jpa.AttributeValueListEntity_;
import org.finra.herd.model.jpa.NamespaceEntity;

@Repository
public class AttributeValueListDaoImpl  extends AbstractHerdDao implements AttributeValueListDao
{
    @Override
    public AttributeValueList getAttributeValueListByKey(AttributeValueListKey attributeValueListKey)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<AttributeValueListEntity> criteria = builder.createQuery(AttributeValueListEntity.class);

        // The criteria root is the tag type code.
        Root<AttributeValueListEntity> attributeValueListEntityRoot = criteria.from(AttributeValueListEntity.class);

        // Create the standard restrictions.
        Predicate queryRestriction = builder.equal(builder
            .upper(attributeValueListEntityRoot.get(AttributeValueListEntity_.attributeValueListName)),
            attributeValueListKey.getAttributeValueListName().toUpperCase());

        // Add all clauses to the query.
        criteria.select(attributeValueListEntityRoot).where(queryRestriction);

        // Run the query and return the results.
        AttributeValueListEntity attributeValueListEntity = entityManager.createQuery(criteria).getSingleResult();
        return new AttributeValueList(attributeValueListEntity.getId(),
            new AttributeValueListKey(attributeValueListEntity.getNamespace().getCode(), attributeValueListEntity.getAttributeValueListName()));
    }

    @Override
    public List<AttributeValueListKey> getAttributeValueListKeyList()
    {
        return getAttributeValueLists().stream()
            .map(p -> new AttributeValueListKey(p.getAttributeValueListKey().getNamespace(),
                p.getAttributeValueListKey().getAttributeValueListName()))
            .collect(Collectors.toList());
    }

    @Override
    public List<AttributeValueList> getAttributeValueLists()
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<AttributeValueListEntity> criteria = builder.createQuery(AttributeValueListEntity.class);

        // The criteria root is the tag type entity.
        Root<AttributeValueListEntity> attributeValueListEntityRoot = criteria.from(AttributeValueListEntity.class);

        // Get the columns.
        Path<String> nameColumn = attributeValueListEntityRoot.get(AttributeValueListEntity_.attributeValueListName);

        // Order the results by tag type's order and display name.
        List<Order> orderBy = new ArrayList<>();
        orderBy.add(builder.asc(nameColumn));

        // Add all clauses to the query.
        criteria.select(attributeValueListEntityRoot).orderBy(orderBy);

        // Run the query and return the results.
        return entityManager.createQuery(criteria)
            .getResultList()
            .stream()
            .map(p -> new AttributeValueList(p.getId(), new AttributeValueListKey(p.getNamespace().getCode(),
                p.getAttributeValueListName())))
            .collect(Collectors.toList());
    }

    @Override
    public AttributeValueListKeys getAttributeValueListKeys()
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<AttributeValueListEntity> criteria = builder.createQuery(AttributeValueListEntity.class);

        // The criteria root is the tag type entity.
        Root<AttributeValueListEntity> attributeValueListEntityRoot = criteria.from(AttributeValueListEntity.class);


        // Join to the other tables we can filter on.
        Join<AttributeValueListEntity, NamespaceEntity> namespaceEntity =
            attributeValueListEntityRoot.join(AttributeValueListEntity_.namespace);


        // Get the columns.
        Path<String> nameColumn = attributeValueListEntityRoot.get(AttributeValueListEntity_.attributeValueListName);

        // Order the results by tag type's order and display name.
        List<Order> orderBy = new ArrayList<>();
        orderBy.add(builder.asc(nameColumn));

        // Add all clauses to the query.
        criteria.select(attributeValueListEntityRoot).orderBy(orderBy);

        // Run the query and return the results.
        List<AttributeValueListEntity> attributeValueListEntityList = entityManager.createQuery(criteria).getResultList();

        List<AttributeValueListKey> attributeValueListKeyList = attributeValueListEntityList
            .stream()
            .map(p -> new AttributeValueListKey(p.getNamespace().getCode(), p.getAttributeValueListName()))
            .collect(Collectors.toList());

        return new AttributeValueListKeys(attributeValueListKeyList);
    }


}
