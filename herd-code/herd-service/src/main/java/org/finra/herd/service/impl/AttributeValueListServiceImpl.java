package org.finra.herd.service.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import org.finra.herd.dao.AttributeValueListDao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.annotation.NamespacePermission;
import org.finra.herd.model.api.xml.Attribute;
import org.finra.herd.model.api.xml.AttributeValueList;
import org.finra.herd.model.api.xml.AttributeValueListCreateRequest;
import org.finra.herd.model.api.xml.AttributeValueListKey;
import org.finra.herd.model.api.xml.AttributeValueListKeys;
import org.finra.herd.model.api.xml.AttributeValueListCreateRequest;
import org.finra.herd.model.api.xml.NamespacePermissionEnum;
import org.finra.herd.model.jpa.AttributeValueListEntity;
import org.finra.herd.model.jpa.AttributeValueListEntity;
import org.finra.herd.model.jpa.DataProviderEntity;
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.service.AttributeValueListService;
import org.finra.herd.service.helper.AlternateKeyHelper;
import org.finra.herd.service.helper.AttributeValueListHelper;
import org.finra.herd.service.helper.NamespaceDaoHelper;


/**
 * The attribute value list service implementation.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class AttributeValueListServiceImpl implements AttributeValueListService
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AttributeValueListServiceImpl.class);

    @Autowired
    private AlternateKeyHelper alternateKeyHelper;

    @Autowired
    private NamespaceDaoHelper namespaceDaoHelper;

    @Autowired
    private AttributeValueListDao attributeValueListDao;

    @Autowired
    private AttributeValueListHelper attributeValueListHelper;

    @NamespacePermission(fields = "#request.namespace", permissions = NamespacePermissionEnum.WRITE)
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public AttributeValueList createAttributeValueList(AttributeValueListCreateRequest attributeValueListCreateRequest)
    {
        // Validate and trim the request parameters.
        validateAttributeValueListCreateRequest(attributeValueListCreateRequest);


        NamespaceEntity namespaceEntity = namespaceDaoHelper.getNamespaceEntity(attributeValueListCreateRequest.getAttributeValueListKey().getNamespace());

        // Validate the tag type does not already exist in the database.
        if (attributeValueListDao.getAttributeValueListByKey(attributeValueListCreateRequest.getAttributeValueListKey()) != null)
        {
            throw new AlreadyExistsException(
                String.format("Unable to create attribute value list with code \"%s\" because it already exists."
                    , attributeValueListCreateRequest.getAttributeValueListKey()));
        }

        // Create and persist a new tag type entity from the request information.
        AttributeValueListEntity attributeValueListEntity = createAttributeValueListEntity(attributeValueListCreateRequest, namespaceEntity);

        // Create and return the tag type object from the persisted entity.
        return new AttributeValueList(attributeValueListEntity.getId(),
            new AttributeValueListKey(attributeValueListEntity.getNamespace().getCode(), attributeValueListEntity.getAttributeValueListName()));
    }

    @NamespacePermission(fields = "#request.namespace", permissions = NamespacePermissionEnum.WRITE)
    @Override
    public AttributeValueList getAttributeValueList(AttributeValueListKey attributeValueListKey)
    {
        return attributeValueListDao.getAttributeValueListByKey(attributeValueListKey);
    }

    @NamespacePermission(fields = "#request.namespace", permissions = NamespacePermissionEnum.WRITE)
    @Override
    public AttributeValueList deleteAttributeValueList(AttributeValueListKey attributeValueListKey)
    {
        // Perform validation and trim.
        attributeValueListHelper.validateAttributeValueListKey(attributeValueListKey);

        // Retrieve and ensure that a tag type already exists with the specified key.
        AttributeValueListEntity attributeValueListEntity = attributeValueListHelper.getAttributeValueListEntity(attributeValueListKey);

        // Delete the tag type.
        attributeValueListDao.delete(attributeValueListKey);

        // Create and return the tag type object from the deleted entity.
        return new AttributeValueList(attributeValueListEntity.getId(),
            new AttributeValueListKey(attributeValueListEntity.getNamespace().getCode(), attributeValueListEntity.getAttributeValueListName()));
    }

    @NamespacePermission(fields = "#request.namespace", permissions = NamespacePermissionEnum.WRITE)
    @Override
    public AttributeValueListKeys getAttributeValueListKeys()
    {
        return (AttributeValueListKeys) attributeValueListDao.getAttributeValueListKeys();
    }

    private AttributeValueListEntity createAttributeValueListEntity(AttributeValueListCreateRequest attributeValueListCreateRequest,
        NamespaceEntity namespaceEntity)
    {
        // Create a new entity.
        AttributeValueListEntity attributeValueListEntity = new AttributeValueListEntity();
        attributeValueListEntity.setNamespace(namespaceEntity);
        attributeValueListEntity.setAttributeValueListName(attributeValueListCreateRequest.getAttributeValueListKey().getAttributeValueListName());

        // Persist and return the new entity.
        return attributeValueListDao.saveAndRefresh(attributeValueListEntity);
    }


    private void validateAttributeValueListCreateRequest(AttributeValueListCreateRequest attributeValueListCreateRequest)
    {
        Assert.notNull(attributeValueListCreateRequest, "A Attribute value list create request must be specified.");
        attributeValueListHelper.validateAttributeValueListKey(attributeValueListCreateRequest.getAttributeValueListKey());
    }


}
