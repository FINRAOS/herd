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

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import org.finra.herd.dao.ExpectedPartitionValueDao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.ExpectedPartitionValueInformation;
import org.finra.herd.model.api.xml.ExpectedPartitionValueKey;
import org.finra.herd.model.api.xml.ExpectedPartitionValuesCreateRequest;
import org.finra.herd.model.api.xml.ExpectedPartitionValuesDeleteRequest;
import org.finra.herd.model.api.xml.ExpectedPartitionValuesInformation;
import org.finra.herd.model.api.xml.PartitionKeyGroupKey;
import org.finra.herd.model.api.xml.PartitionValueRange;
import org.finra.herd.model.jpa.ExpectedPartitionValueEntity;
import org.finra.herd.model.jpa.PartitionKeyGroupEntity;
import org.finra.herd.service.ExpectedPartitionValueService;
import org.finra.herd.service.helper.AlternateKeyHelper;
import org.finra.herd.service.helper.ExpectedPartitionValueHelper;
import org.finra.herd.service.helper.PartitionKeyGroupDaoHelper;
import org.finra.herd.service.helper.PartitionKeyGroupHelper;

/**
 * The partition key group service implementation.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class ExpectedPartitionValueServiceImpl implements ExpectedPartitionValueService
{
    @Autowired
    private AlternateKeyHelper alternateKeyHelper;

    @Autowired
    private ExpectedPartitionValueDao expectedPartitionValueDao;

    @Autowired
    private ExpectedPartitionValueHelper expectedPartitionValueHelper;

    @Autowired
    private PartitionKeyGroupDaoHelper partitionKeyGroupDaoHelper;

    @Autowired
    private PartitionKeyGroupHelper partitionKeyGroupHelper;

    /**
     * Creates a list of expected partition values for an existing partition key group.
     *
     * @param expectedPartitionValuesCreateRequest the information needed to create the expected partition values
     *
     * @return the newly created expected partition values
     */
    @Override
    public ExpectedPartitionValuesInformation createExpectedPartitionValues(ExpectedPartitionValuesCreateRequest expectedPartitionValuesCreateRequest)
    {
        // Perform request validation and trim request parameters.
        validateExpectedPartitionValuesCreateRequest(expectedPartitionValuesCreateRequest);

        // Retrieve and ensure that a partition key group exists with the specified name.
        PartitionKeyGroupEntity partitionKeyGroupEntity =
            partitionKeyGroupDaoHelper.getPartitionKeyGroupEntity(expectedPartitionValuesCreateRequest.getPartitionKeyGroupKey());

        // Load all existing expected partition value entities into a map for quick access.
        Map<String, ExpectedPartitionValueEntity> expectedPartitionValueEntityMap =
            getExpectedPartitionValueEntityMap(partitionKeyGroupEntity.getExpectedPartitionValues());

        // Fail if any of the expected partition values to be created already exist.
        for (String expectedPartitionValue : expectedPartitionValuesCreateRequest.getExpectedPartitionValues())
        {
            if (expectedPartitionValueEntityMap.containsKey(expectedPartitionValue))
            {
                throw new AlreadyExistsException(String
                    .format("Expected partition value \"%s\" already exists in \"%s\" partition key group.", expectedPartitionValue,
                        partitionKeyGroupEntity.getPartitionKeyGroupName()));
            }
        }

        // Create and persist the expected partition value entities.
        Collection<ExpectedPartitionValueEntity> createdExpectedPartitionValueEntities = new ArrayList<>();
        for (String expectedPartitionValue : expectedPartitionValuesCreateRequest.getExpectedPartitionValues())
        {
            ExpectedPartitionValueEntity expectedPartitionValueEntity = new ExpectedPartitionValueEntity();
            createdExpectedPartitionValueEntities.add(expectedPartitionValueEntity);
            expectedPartitionValueEntity.setPartitionKeyGroup(partitionKeyGroupEntity);
            partitionKeyGroupEntity.getExpectedPartitionValues().add(expectedPartitionValueEntity);
            expectedPartitionValueEntity.setPartitionValue(expectedPartitionValue);
            expectedPartitionValueDao.saveAndRefresh(expectedPartitionValueEntity);
        }
        
        return createExpectedPartitionValuesInformationFromEntities(partitionKeyGroupEntity, createdExpectedPartitionValueEntities);
    }

    /**
     * Retrieves an existing expected partition value plus/minus an optional offset. This method starts a new transaction.
     *
     * @param expectedPartitionValueKey the expected partition value key
     * @param offset the optional positive or negative offset
     *
     * @return the expected partition value
     */
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public ExpectedPartitionValueInformation getExpectedPartitionValue(ExpectedPartitionValueKey expectedPartitionValueKey, Integer offset)
    {
        return getExpectedPartitionValueImpl(expectedPartitionValueKey, offset);
    }

    /**
     * Retrieves an existing expected partition value plus/minus an optional offset.
     *
     * @param expectedPartitionValueKey the expected partition value key
     * @param offset the optional positive or negative offset
     *
     * @return the expected partition value
     */
    protected ExpectedPartitionValueInformation getExpectedPartitionValueImpl(ExpectedPartitionValueKey expectedPartitionValueKey, Integer offset)
    {
        // Perform validation and trim of the input parameters.
        expectedPartitionValueHelper.validateExpectedPartitionValueKey(expectedPartitionValueKey);

        // Retrieve and ensure that a partition key group exists with the specified name.
        partitionKeyGroupDaoHelper.getPartitionKeyGroupEntity(expectedPartitionValueKey.getPartitionKeyGroupName());

        // Retrieve the start expected partition value by passing 0 offset value.
        ExpectedPartitionValueEntity expectedPartitionValueEntity = expectedPartitionValueDao.getExpectedPartitionValue(expectedPartitionValueKey, 0);

        if (expectedPartitionValueEntity == null)
        {
            throw new ObjectNotFoundException(String
                .format("Expected partition value \"%s\" doesn't exist in \"%s\" partition key group.", expectedPartitionValueKey.getExpectedPartitionValue(),
                    expectedPartitionValueKey.getPartitionKeyGroupName()));
        }

        // If we have a non-zero offset, retrieve the offset expected partition value.
        if (offset != null && offset != 0)
        {
            expectedPartitionValueEntity = expectedPartitionValueDao.getExpectedPartitionValue(expectedPartitionValueKey, offset);

            if (expectedPartitionValueEntity == null)
            {
                throw new ObjectNotFoundException(String.format("Expected partition value \"%s\" with offset %d doesn't exist in \"%s\" partition key group.",
                    expectedPartitionValueKey.getExpectedPartitionValue(), offset, expectedPartitionValueKey.getPartitionKeyGroupName()));
            }
        }

        return createExpectedPartitionValueInformationFromEntity(expectedPartitionValueEntity);
    }

    /**
     * Retrieves a range of existing expected partition values.
     *
     * @param partitionKeyGroupKey the partition key group key
     * @param partitionValueRange the partition value range
     *
     * @return the expected partition values
     */
    @Override
    public ExpectedPartitionValuesInformation getExpectedPartitionValues(PartitionKeyGroupKey partitionKeyGroupKey, PartitionValueRange partitionValueRange)
    {
        // Perform validation and trim of the input parameters.
        partitionKeyGroupHelper.validatePartitionKeyGroupKey(partitionKeyGroupKey);

        // Trim the start expected partition value for the expected partition value range.
        if (StringUtils.isNotBlank(partitionValueRange.getStartPartitionValue()))
        {
            partitionValueRange.setStartPartitionValue(partitionValueRange.getStartPartitionValue().trim());
        }

        // Trim the end expected partition value for the expected partition value range.
        if (StringUtils.isNotBlank(partitionValueRange.getEndPartitionValue()))
        {
            partitionValueRange.setEndPartitionValue(partitionValueRange.getEndPartitionValue().trim());
        }

        // Validate that we are not missing both start and end expected partition values for the range.
        if (StringUtils.isBlank(partitionValueRange.getStartPartitionValue()) && StringUtils.isBlank(partitionValueRange.getEndPartitionValue()))
        {
            throw new IllegalArgumentException("At least one start or end expected partition value must be specified.");
        }

        // Using string compare, validate that start expected partition value is less than or equal to end expected partition value.
        if (StringUtils.isNotBlank(partitionValueRange.getStartPartitionValue()) && StringUtils.isNotBlank(partitionValueRange.getEndPartitionValue()) &&
            partitionValueRange.getStartPartitionValue().compareTo(partitionValueRange.getEndPartitionValue()) > 0)
        {
            throw new IllegalArgumentException(String
                .format("The start expected partition value \"%s\" cannot be greater than the end expected partition value \"%s\".",
                    partitionValueRange.getStartPartitionValue(), partitionValueRange.getEndPartitionValue()));
        }

        // Retrieve and ensure that a partition key group exists with the specified name.
        PartitionKeyGroupEntity partitionKeyGroupEntity = partitionKeyGroupDaoHelper.getPartitionKeyGroupEntity(partitionKeyGroupKey);

        // Retrieve a list of expected partition values.
        Collection<ExpectedPartitionValueEntity> expectedPartitionValueEntities =
            expectedPartitionValueDao.getExpectedPartitionValuesByGroupAndRange(partitionKeyGroupKey.getPartitionKeyGroupName(), partitionValueRange);

        return createExpectedPartitionValuesInformationFromEntities(partitionKeyGroupEntity, expectedPartitionValueEntities);
    }

    /**
     * Deletes specified expected partition values from an existing partition key group which is identified by name.
     *
     * @param expectedPartitionValuesDeleteRequest the information needed to delete the expected partition values
     *
     * @return the expected partition values that got deleted
     */
    @Override
    public ExpectedPartitionValuesInformation deleteExpectedPartitionValues(ExpectedPartitionValuesDeleteRequest expectedPartitionValuesDeleteRequest)
    {
        // Perform request validation and trim request parameters.
        validateExpectedPartitionValuesDeleteRequest(expectedPartitionValuesDeleteRequest);

        // Retrieve and ensure that a partition key group exists with the specified name.
        PartitionKeyGroupEntity partitionKeyGroupEntity =
            partitionKeyGroupDaoHelper.getPartitionKeyGroupEntity(expectedPartitionValuesDeleteRequest.getPartitionKeyGroupKey());

        // Load all existing expected partition value entities into a map for quick access.
        Map<String, ExpectedPartitionValueEntity> expectedPartitionValueEntityMap =
            getExpectedPartitionValueEntityMap(partitionKeyGroupEntity.getExpectedPartitionValues());

        // Build a list of all expected partition value entities to be deleted.
        Collection<ExpectedPartitionValueEntity> deletedExpectedPartitionValueEntities = new ArrayList<>();
        for (String expectedPartitionValue : expectedPartitionValuesDeleteRequest.getExpectedPartitionValues())
        {
            // Find the relative expected partition entity.
            ExpectedPartitionValueEntity expectedPartitionValueEntity = expectedPartitionValueEntityMap.get(expectedPartitionValue);
            if (expectedPartitionValueEntity != null)
            {
                deletedExpectedPartitionValueEntities.add(expectedPartitionValueEntity);
            }
            else
            {
                throw new ObjectNotFoundException(String
                    .format("Expected partition value \"%s\" doesn't exist in \"%s\" partition key group.", expectedPartitionValue,
                        partitionKeyGroupEntity.getPartitionKeyGroupName()));
            }
        }

        // Perform the actual deletion.
        for (ExpectedPartitionValueEntity expectedPartitionValueEntity : deletedExpectedPartitionValueEntities)
        {
            partitionKeyGroupEntity.getExpectedPartitionValues().remove(expectedPartitionValueEntity);
            expectedPartitionValueDao.delete(expectedPartitionValueEntity);
        }

        return createExpectedPartitionValuesInformationFromEntities(partitionKeyGroupEntity, deletedExpectedPartitionValueEntities);
    }

    /**
     * Validates the expected partition values create request. This method also trims request parameters.
     *
     * @param expectedPartitionValuesCreateRequest the expected partition values create request
     *
     * @throws IllegalArgumentException if any validation errors were found
     */

    private void validateExpectedPartitionValuesCreateRequest(ExpectedPartitionValuesCreateRequest expectedPartitionValuesCreateRequest)
    {
        // Perform validation and trim of the partition key group key.
        partitionKeyGroupHelper.validatePartitionKeyGroupKey(expectedPartitionValuesCreateRequest.getPartitionKeyGroupKey());

        // Perform validation and trim of the expected partition values.
        expectedPartitionValuesCreateRequest
            .setExpectedPartitionValues(validateExpectedPartitionValues(expectedPartitionValuesCreateRequest.getExpectedPartitionValues()));
    }

    /**
     * Validates the expected partition values delete request. This method also trims request parameters.
     *
     * @param expectedPartitionValuesDeleteRequest the expected partition values delete request
     *
     * @throws IllegalArgumentException if any validation errors were found
     */
    private void validateExpectedPartitionValuesDeleteRequest(ExpectedPartitionValuesDeleteRequest expectedPartitionValuesDeleteRequest)
    {
        // Perform validation and trim of the partition key group key.
        partitionKeyGroupHelper.validatePartitionKeyGroupKey(expectedPartitionValuesDeleteRequest.getPartitionKeyGroupKey());

        // Perform validation and trim of expected partition values.
        expectedPartitionValuesDeleteRequest
            .setExpectedPartitionValues(validateExpectedPartitionValues(expectedPartitionValuesDeleteRequest.getExpectedPartitionValues()));
    }

    /**
     * Validate a list of expected partition values. This method also trims the expected partition values.
     *
     * @param expectedPartitionValues the list of expected partition values
     *
     * @return the validated and sorted list of expected partition values
     * @throws IllegalArgumentException if any validation errors were found
     */
    private List<String> validateExpectedPartitionValues(List<String> expectedPartitionValues)
    {
        Assert.notEmpty(expectedPartitionValues, "At least one expected partition value must be specified.");

        // Ensure the expected partition value isn't a duplicate by using a hash set.
        Set<String> validatedExpectedPartitionValuesSet = new LinkedHashSet<>();
        for (String expectedPartitionValue : expectedPartitionValues)
        {
            String trimmedExpectedPartitionValue = alternateKeyHelper.validateStringParameter("An", "expected partition value", expectedPartitionValue);

            if (validatedExpectedPartitionValuesSet.contains(trimmedExpectedPartitionValue))
            {
                throw new IllegalArgumentException(String.format("Duplicate expected partition value \"%s\" found.", trimmedExpectedPartitionValue));
            }

            validatedExpectedPartitionValuesSet.add(trimmedExpectedPartitionValue);
        }

        List<String> validatedExpectedPartitionValues = new ArrayList<>(validatedExpectedPartitionValuesSet);

        // Sort the expected partition values list.
        Collections.sort(validatedExpectedPartitionValues);

        // Return the updated expected partition value list.
        return validatedExpectedPartitionValues;
    }

    /**
     * Creates a map that maps expected partition values to the relative expected partition value entities.
     *
     * @param expectedPartitionValueEntities the collection of expected partition value entities to be loaded into the map
     *
     * @return the map that maps expected partition values to the relative expected partition value entities
     */
    private Map<String, ExpectedPartitionValueEntity> getExpectedPartitionValueEntityMap(
        Collection<ExpectedPartitionValueEntity> expectedPartitionValueEntities)
    {
        Map<String, ExpectedPartitionValueEntity> expectedPartitionValueEntityMap = new HashMap<>();

        for (ExpectedPartitionValueEntity expectedPartitionValueEntity : expectedPartitionValueEntities)
        {
            expectedPartitionValueEntityMap.put(expectedPartitionValueEntity.getPartitionValue(), expectedPartitionValueEntity);
        }

        return expectedPartitionValueEntityMap;
    }

    /**
     * Creates the expected partition value information from the persisted entity.
     *
     * @param expectedPartitionValueEntity the expected partition value entity
     *
     * @return the expected partition value information
     */
    private ExpectedPartitionValueInformation createExpectedPartitionValueInformationFromEntity(ExpectedPartitionValueEntity expectedPartitionValueEntity)
    {
        // Create an expected partition values information instance.
        ExpectedPartitionValueInformation expectedPartitionValueInformation = new ExpectedPartitionValueInformation();

        // Add the expected partition value key.
        ExpectedPartitionValueKey expectedPartitionValueKey = new ExpectedPartitionValueKey();
        expectedPartitionValueInformation.setExpectedPartitionValueKey(expectedPartitionValueKey);
        expectedPartitionValueKey.setPartitionKeyGroupName(expectedPartitionValueEntity.getPartitionKeyGroup().getPartitionKeyGroupName());
        expectedPartitionValueKey.setExpectedPartitionValue(expectedPartitionValueEntity.getPartitionValue());

        return expectedPartitionValueInformation;
    }

    /**
     * Creates the expected partition values information from the persisted entities.
     *
     * @param partitionKeyGroupEntity the partition key group entity
     * @param expectedPartitionValueEntities the list of expected partition value entities
     *
     * @return the expected partition values information
     */
    private ExpectedPartitionValuesInformation createExpectedPartitionValuesInformationFromEntities(PartitionKeyGroupEntity partitionKeyGroupEntity,
        Collection<ExpectedPartitionValueEntity> expectedPartitionValueEntities)
    {
        // Create an expected partition values information instance.
        ExpectedPartitionValuesInformation expectedPartitionValuesInformation = new ExpectedPartitionValuesInformation();

        // Add the partition key group key.
        PartitionKeyGroupKey partitionKeyGroupKey = new PartitionKeyGroupKey();
        expectedPartitionValuesInformation.setPartitionKeyGroupKey(partitionKeyGroupKey);
        partitionKeyGroupKey.setPartitionKeyGroupName(partitionKeyGroupEntity.getPartitionKeyGroupName());

        // Add the expected partition values.
        List<String> expectedPartitionValues = new ArrayList<>();
        expectedPartitionValuesInformation.setExpectedPartitionValues(expectedPartitionValues);

        for (ExpectedPartitionValueEntity expectedPartitionValueEntity : expectedPartitionValueEntities)
        {
            expectedPartitionValues.add(expectedPartitionValueEntity.getPartitionValue());
        }

        return expectedPartitionValuesInformation;
    }
}
