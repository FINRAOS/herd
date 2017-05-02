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
package org.finra.herd.service.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import org.finra.herd.dao.AllowedAttributeValueDao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.annotation.NamespacePermission;
import org.finra.herd.model.api.xml.AllowedAttributeValuesCreateRequest;
import org.finra.herd.model.api.xml.AllowedAttributeValuesDeleteRequest;
import org.finra.herd.model.api.xml.AllowedAttributeValuesInformation;
import org.finra.herd.model.api.xml.AttributeValueListKey;
import org.finra.herd.model.api.xml.NamespacePermissionEnum;
import org.finra.herd.model.jpa.AllowedAttributeValueEntity;
import org.finra.herd.model.jpa.AttributeValueListEntity;
import org.finra.herd.service.AllowedAttributeValueService;
import org.finra.herd.service.helper.AlternateKeyHelper;
import org.finra.herd.service.helper.AttributeValueListHelper;

/**
 * The allowed attribute values service implementation.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class AllowedAttributeValueServiceImpl implements AllowedAttributeValueService
{
    @Autowired
    private AlternateKeyHelper alternateKeyHelper;

    @Autowired
    private AllowedAttributeValueDao allowedAttributeValueDao;

    @Autowired
    private AttributeValueListHelper attributeValueListHelper;

    /**
     * Creates a list of allowed attribute values for an existing attribute value list key.
     *
     * @param request the information needed to create the allowed attribute values
     *
     * @return the newly created allowed attribute values
     */
    @NamespacePermission(fields = "#request.attributeValueListKey.namespace", permissions = NamespacePermissionEnum.WRITE)
    @Override
    public AllowedAttributeValuesInformation createAllowedAttributeValues(AllowedAttributeValuesCreateRequest request)
    {
        // Perform request validation and trim request parameters.
        validateAllowedAttributeValuesCreateRequest(request);

        // Retrieve and ensure that a attribute value list exists with the specified name.
        AttributeValueListEntity attributeValueListEntity = attributeValueListHelper.getAttributeValueListEntity(request.getAttributeValueListKey());

        // Load all existing allowed attribute value entities into a map for quick access.
        Map<String, AllowedAttributeValueEntity> allowedAttributeValueEntityMap =
            getAllowedAttributeValueEntityMap(attributeValueListEntity.getAllowedAttributeValues());

        // Fail if any of the allowed attribute values to be created already exist.
        for (String allowedAttributeValue : request.getAllowedAttributeValues())
        {
            if (allowedAttributeValueEntityMap.containsKey(allowedAttributeValue))
            {
                throw new IllegalArgumentException(String
                    .format("Allowed attribute value \"%s\" already exists in \"%s\" attribute value list.", allowedAttributeValue,
                        attributeValueListEntity.getAttributeValueListName()));
            }
        }

        // Create and persist the allowed attribute value entities.
        Collection<AllowedAttributeValueEntity> createdAllowedAttributeValueEntities = new ArrayList<>();
        for (String allowedAttributeValue : request.getAllowedAttributeValues())
        {
            AllowedAttributeValueEntity allowedAttributeValueEntity = new AllowedAttributeValueEntity();
            createdAllowedAttributeValueEntities.add(allowedAttributeValueEntity);
            allowedAttributeValueEntity.setAttributeValueListEntity(attributeValueListEntity);
            allowedAttributeValueEntity.setAllowedAttributeValue(allowedAttributeValue);
            allowedAttributeValueDao.saveAndRefresh(allowedAttributeValueEntity);
        }
        allowedAttributeValueDao.saveAndRefresh(attributeValueListEntity);

        return createAllowedAttributeValuesInformationFromEntities(attributeValueListEntity, createdAllowedAttributeValueEntities);
    }

    /**
     * Retrieves existing allowed attribute values based on the specified key
     *
     * @param attributeValueListKey the attribute value list key
     *
     * @return the allowed attribute values information
     */
    @NamespacePermission(fields = "#attributeValueListKey.namespace", permissions = NamespacePermissionEnum.READ)
    @Override
    public AllowedAttributeValuesInformation getAllowedAttributeValues(AttributeValueListKey attributeValueListKey)
    {
        // Perform validation and trim of the input parameters.
        attributeValueListHelper.validateAttributeValueListKey(attributeValueListKey);

        // Retrieve and ensure that a attribute value list exists with the specified name.
        AttributeValueListEntity attributeValueListEntity = attributeValueListHelper.getAttributeValueListEntity(attributeValueListKey);

        // Retrieve a list of allowed attribute values.
        List<AllowedAttributeValueEntity> allowedAttributeValueEntities =
            allowedAttributeValueDao.getAllowedAttributeValuesByAttributeValueListKey(attributeValueListKey);

        return createAllowedAttributeValuesInformationFromEntities(attributeValueListEntity, allowedAttributeValueEntities);
    }

    /**
     * Deletes specified allowed attribute values from an existing attribute value list which is identified by name.
     *
     * @param request the information needed to delete the allowed attribute values
     *
     * @return the allowed attribute values that got deleted
     */
    @NamespacePermission(fields = "#request.attributeValueListKey.namespace", permissions = NamespacePermissionEnum.WRITE)
    @Override
    public AllowedAttributeValuesInformation deleteAllowedAttributeValues(AllowedAttributeValuesDeleteRequest request)
    {
        // Perform request validation and trim request parameters.
        validateAllowedAttributeValuesDeleteRequest(request);

        // Retrieve and ensure that a attribute value list exists with the specified name.
        AttributeValueListEntity attributeValueListEntity = attributeValueListHelper.getAttributeValueListEntity(request.getAttributeValueListKey());

        // Load all existing allowed attribute value entities into a map for quick access.
        Map<String, AllowedAttributeValueEntity> allowedAttributeValueEntityMap =
            getAllowedAttributeValueEntityMap(attributeValueListEntity.getAllowedAttributeValues());

        // Build a list of all allowed attribute value entities to be deleted.
        Collection<AllowedAttributeValueEntity> deletedAllowedAttributeValueEntities = new ArrayList<>();
        for (String allowedAttributeValue : request.getAllowedAttributeValues())
        {
            // Find the relative allowed attribute entity.
            AllowedAttributeValueEntity allowedAttributeValueEntity = allowedAttributeValueEntityMap.get(allowedAttributeValue);
            if (allowedAttributeValueEntity != null)
            {
                deletedAllowedAttributeValueEntities.add(allowedAttributeValueEntity);
            }
            else
            {
                throw new ObjectNotFoundException(String
                    .format("Allowed attribute value \"%s\" doesn't exist in \"%s\" attribute value list.", allowedAttributeValue,
                        attributeValueListEntity.getAttributeValueListName()));
            }
        }

        // Perform the actual deletion.
        for (AllowedAttributeValueEntity allowedAttributeValueEntity : deletedAllowedAttributeValueEntities)
        {
            attributeValueListEntity.getAllowedAttributeValues().remove(allowedAttributeValueEntity);
        }
        allowedAttributeValueDao.saveAndRefresh(attributeValueListEntity);

        return createAllowedAttributeValuesInformationFromEntities(attributeValueListEntity, deletedAllowedAttributeValueEntities);
    }

    /**
     * Validates the allowed attribute values create request. This method also trims request parameters.
     *
     * @param request the allowed attribute values create request
     *
     * @throws IllegalArgumentException if any validation errors were found
     */

    private void validateAllowedAttributeValuesCreateRequest(AllowedAttributeValuesCreateRequest request)
    {
        Assert.notNull(request, "An allowed attribute value create request must be specified.");

        // Perform validation and trim of the attribute value list key.
        attributeValueListHelper.validateAttributeValueListKey(request.getAttributeValueListKey());

        // Perform validation and trim of the allowed attribute values.
        request.setAllowedAttributeValues(validateAllowedAttributeValues(request.getAllowedAttributeValues()));
    }

    /**
     * Validates the allowed attribute values delete request. This method also trims request parameters.
     *
     * @param request the allowed attribute values delete request
     *
     * @throws IllegalArgumentException if any validation errors were found
     */
    private void validateAllowedAttributeValuesDeleteRequest(AllowedAttributeValuesDeleteRequest request)
    {
        Assert.notNull(request, "An allowed attribute value delete request must be specified.");

        // Perform validation and trim of the attribute value list key.
        attributeValueListHelper.validateAttributeValueListKey(request.getAttributeValueListKey());

        // Perform validation and trim of allowed attribute values.
        request.setAllowedAttributeValues(validateAllowedAttributeValues(request.getAllowedAttributeValues()));
    }

    /**
     * Creates a map that maps allowed attribute values to the relative allowed attribute value entities.
     *
     * @param allowedAttributeValueEntities the collection of allowed attribute value entities to be loaded into the map
     *
     * @return the map that maps allowed attribute values to the relative allowed attribute value entities
     */
    private Map<String, AllowedAttributeValueEntity> getAllowedAttributeValueEntityMap(Collection<AllowedAttributeValueEntity> allowedAttributeValueEntities)
    {
        Map<String, AllowedAttributeValueEntity> allowedAttributeValueEntityMap = new HashMap<>();

        allowedAttributeValueEntities.forEach((allowedAttributeValueEntity) -> {
            allowedAttributeValueEntityMap.put(allowedAttributeValueEntity.getAllowedAttributeValue(), allowedAttributeValueEntity);
        });

        return allowedAttributeValueEntityMap;
    }

    /**
     * Validate a list of allowed attribute values. This method also trims the allowed attribute values.
     *
     * @param allowedAttributeValues the list of allowed attribute values
     *
     * @return the validated and sorted list of allowed attribute values
     * @throws IllegalArgumentException if any validation errors were found
     */
    private List<String> validateAllowedAttributeValues(List<String> allowedAttributeValues)
    {
        Assert.notEmpty(allowedAttributeValues, "At least one allowed attribute value must be specified.");

        // Ensure the allowed attribute value isn't a duplicate by using a hash set.
        Set<String> validatedAllowedAttributeValuesSet = new LinkedHashSet<>();
        for (String allowedAttributeValue : allowedAttributeValues)
        {
            String trimmedAllowedAttributeValue = alternateKeyHelper.validateStringParameter("An", "allowed attribute value", allowedAttributeValue);

            if (validatedAllowedAttributeValuesSet.contains(trimmedAllowedAttributeValue))
            {
                throw new IllegalArgumentException(String.format("Duplicate allowed attribute value \"%s\" found.", trimmedAllowedAttributeValue));
            }

            validatedAllowedAttributeValuesSet.add(trimmedAllowedAttributeValue);
        }

        List<String> validatedAllowedAttributeValues = new ArrayList<>(validatedAllowedAttributeValuesSet);

        // Sort the allowed attribute values list.
        Collections.sort(validatedAllowedAttributeValues);

        // Return the updated allowed attribute value list.
        return validatedAllowedAttributeValues;
    }

    /**
     * Creates the allowed attribute values information from the persisted entities.
     *
     * @param attributeValueListEntity the attribute value list entity
     * @param allowedAttributeValueEntities the list of allowed attribute value entities
     *
     * @return the allowed attribute values information
     */
    private AllowedAttributeValuesInformation createAllowedAttributeValuesInformationFromEntities(AttributeValueListEntity attributeValueListEntity,
        Collection<AllowedAttributeValueEntity> allowedAttributeValueEntities)
    {
        // Create an allowed attribute values information instance.
        AllowedAttributeValuesInformation allowedAttributeValuesInformation = new AllowedAttributeValuesInformation();

        // Add the attribute value list key.
        AttributeValueListKey attributeValueListKey = new AttributeValueListKey();
        allowedAttributeValuesInformation.setAttributeValueListKey(attributeValueListKey);
        attributeValueListKey.setNamespace(attributeValueListEntity.getNamespace().getCode());
        attributeValueListKey.setAttributeValueListName(attributeValueListEntity.getAttributeValueListName());

        // Add the allowed attribute values.
        List<String> allowedAttributeValues = new ArrayList<>();
        allowedAttributeValuesInformation.setAllowedAttributeValues(allowedAttributeValues);

        allowedAttributeValueEntities.forEach(allowedAttributeValueEntity -> {
            allowedAttributeValues.add(allowedAttributeValueEntity.getAllowedAttributeValue());
        });

        return allowedAttributeValuesInformation;
    }
}
