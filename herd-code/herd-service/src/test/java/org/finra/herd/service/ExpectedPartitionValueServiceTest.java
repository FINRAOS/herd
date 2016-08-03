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
package org.finra.herd.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.ExpectedPartitionValueInformation;
import org.finra.herd.model.api.xml.ExpectedPartitionValueKey;
import org.finra.herd.model.api.xml.ExpectedPartitionValuesCreateRequest;
import org.finra.herd.model.api.xml.ExpectedPartitionValuesDeleteRequest;
import org.finra.herd.model.api.xml.ExpectedPartitionValuesInformation;
import org.finra.herd.model.api.xml.PartitionKeyGroupKey;
import org.finra.herd.model.api.xml.PartitionValueRange;
import org.finra.herd.model.jpa.PartitionKeyGroupEntity;

/**
 * This class tests various functionality within the expected partition value REST controller.
 */
public class ExpectedPartitionValueServiceTest extends AbstractServiceTest
{
    @Autowired
    @Qualifier(value = "expectedPartitionValueServiceImpl")
    private ExpectedPartitionValueService expectedPartitionValueServiceImpl;

    @Test
    public void testCreateExpectedPartitionValues()
    {
        // Create and persist a partition key group entity.
        partitionKeyGroupDaoTestHelper.createPartitionKeyGroupEntity(PARTITION_KEY_GROUP);

        // Add expected partition values to this partition key group.
        ExpectedPartitionValuesCreateRequest request =
            createExpectedPartitionValuesCreateRequest(PARTITION_KEY_GROUP, expectedPartitionValueDaoTestHelper.getTestUnsortedExpectedPartitionValues());
        ExpectedPartitionValuesInformation resultPartitionValuesInformation = expectedPartitionValueService.createExpectedPartitionValues(request);

        // Validate the returned object.
        validateExpectedPartitionValuesInformation(PARTITION_KEY_GROUP, expectedPartitionValueDaoTestHelper.getTestSortedExpectedPartitionValues(),
            resultPartitionValuesInformation);
    }

    @Test
    public void testCreateExpectedPartitionValuesMissingRequiredParameters()
    {
        ExpectedPartitionValuesCreateRequest request;

        // Try to perform a create without specifying partition key group name.
        request = createExpectedPartitionValuesCreateRequest(BLANK_TEXT, expectedPartitionValueDaoTestHelper.getTestUnsortedExpectedPartitionValues());
        try
        {
            expectedPartitionValueService.createExpectedPartitionValues(request);
            fail("Should throw an IllegalArgumentException when partition key group is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A partition key group name must be specified.", e.getMessage());
        }

        // Try to perform a create without specifying any expected partition values.
        request = createExpectedPartitionValuesCreateRequest(PARTITION_KEY_GROUP, new ArrayList<String>());
        try
        {
            expectedPartitionValueService.createExpectedPartitionValues(request);
            fail("Should throw an IllegalArgumentException when no expected partition values are specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("At least one expected partition value must be specified.", e.getMessage());
        }

        // Try to perform a create with a missing expected partition value.
        request = createExpectedPartitionValuesCreateRequest(PARTITION_KEY_GROUP, Arrays.asList(BLANK_TEXT));
        try
        {
            expectedPartitionValueService.createExpectedPartitionValues(request);
            fail("Should throw an IllegalArgumentException when expected partition value is missing.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("An expected partition value must be specified.", e.getMessage());
        }
    }

    @Test
    public void testCreateExpectedPartitionValuesTrimParameters()
    {
        // Create and persist a partition key group entity.
        partitionKeyGroupDaoTestHelper.createPartitionKeyGroupEntity(PARTITION_KEY_GROUP);

        // Add expected partition values to this partition key group with request parameters padded with whitespace characters.
        ExpectedPartitionValuesCreateRequest request = createExpectedPartitionValuesCreateRequest(addWhitespace(PARTITION_KEY_GROUP),
            expectedPartitionValueDaoTestHelper.getTestUnsortedExpectedPartitionValues());
        for (int i = 0; i < request.getExpectedPartitionValues().size(); i++)
        {
            request.getExpectedPartitionValues().set(i, addWhitespace(request.getExpectedPartitionValues().get(i)));
        }
        ExpectedPartitionValuesInformation resultPartitionValuesInformation = expectedPartitionValueService.createExpectedPartitionValues(request);

        // Validate the returned object.
        validateExpectedPartitionValuesInformation(PARTITION_KEY_GROUP, expectedPartitionValueDaoTestHelper.getTestSortedExpectedPartitionValues(),
            resultPartitionValuesInformation);
    }

    @Test
    public void testCreateExpectedPartitionValuesInvalidParameters()
    {
        // Create and persist a partition key group entity.
        partitionKeyGroupDaoTestHelper.createPartitionKeyGroupEntity(PARTITION_KEY_GROUP);

        // Try to perform a create when partition key group name contains a forward slash character.
        try
        {
            expectedPartitionValueService.createExpectedPartitionValues(createExpectedPartitionValuesCreateRequest(addSlash(PARTITION_KEY_GROUP),
                expectedPartitionValueDaoTestHelper.getTestUnsortedExpectedPartitionValues()));
            fail("Should throw an IllegalArgumentException when partition key group name contains a forward slash character.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Partition key group name can not contain a forward slash character.", e.getMessage());
        }

        // Try to perform a create when expected partition value contains a forward slash character.
        try
        {
            expectedPartitionValueService
                .createExpectedPartitionValues(createExpectedPartitionValuesCreateRequest(PARTITION_KEY_GROUP, Arrays.asList(addSlash(PARTITION_VALUE))));
            fail("Should throw an IllegalArgumentException when expected partition value contains a forward slash character.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Expected partition value can not contain a forward slash character.", e.getMessage());
        }
    }

    @Test
    public void testCreateExpectedPartitionValuesDuplicatePartitionValues()
    {
        // Try to perform a create by passing duplicate expected partition values.
        ExpectedPartitionValuesCreateRequest request =
            createExpectedPartitionValuesCreateRequest(PARTITION_KEY_GROUP, Arrays.asList(PARTITION_VALUE, PARTITION_VALUE));
        try
        {
            expectedPartitionValueService.createExpectedPartitionValues(request);
            fail("Should throw an IllegalArgumentException when create request contains duplicate expected partition values.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Duplicate expected partition value \"%s\" found.", PARTITION_VALUE), e.getMessage());
        }
    }

    @Test
    public void testCreateExpectedPartitionValuesPartitionKeyGroupNoExists()
    {
        // Try to perform a create using a non-existing partition key group name.
        ExpectedPartitionValuesCreateRequest request =
            createExpectedPartitionValuesCreateRequest("I_DO_NOT_EXIST", expectedPartitionValueDaoTestHelper.getTestUnsortedExpectedPartitionValues());
        try
        {
            expectedPartitionValueService.createExpectedPartitionValues(request);
            fail("Should throw an IllegalArgumentException when partition key group does not exist.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Partition key group \"%s\" doesn't exist.", request.getPartitionKeyGroupKey().getPartitionKeyGroupName()),
                e.getMessage());
        }
    }

    @Test
    public void testCreateExpectedPartitionValuesExpectedPartitionValueAlreadyExists()
    {
        // Create and persist a partition key group entity.
        PartitionKeyGroupEntity partitionKeyGroupEntity = partitionKeyGroupDaoTestHelper.createPartitionKeyGroupEntity(PARTITION_KEY_GROUP);

        // Create and persist a single test expected partition value.
        expectedPartitionValueDaoTestHelper.createExpectedPartitionValueEntities(partitionKeyGroupEntity, Arrays.asList(PARTITION_VALUE));

        // Try to perform a create using an already existing expected partition value.
        ExpectedPartitionValuesCreateRequest request = createExpectedPartitionValuesCreateRequest(PARTITION_KEY_GROUP, Arrays.asList(PARTITION_VALUE));
        try
        {
            expectedPartitionValueService.createExpectedPartitionValues(request);
            fail("Should throw an IllegalArgumentException when expected partition value already exists.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Expected partition value \"%s\" already exists in \"%s\" partition key group.", PARTITION_VALUE, PARTITION_KEY_GROUP),
                e.getMessage());
        }
    }

    @Test
    public void testGetExpectedPartitionValue()
    {
        // Create and persist a partition key group entity.
        PartitionKeyGroupEntity partitionKeyGroupEntity = partitionKeyGroupDaoTestHelper.createPartitionKeyGroupEntity(PARTITION_KEY_GROUP);

        // Create and persist a list of test expected partition values.
        expectedPartitionValueDaoTestHelper
            .createExpectedPartitionValueEntities(partitionKeyGroupEntity, expectedPartitionValueDaoTestHelper.getTestUnsortedExpectedPartitionValues());

        // Get expected partition value for different offset values.
        List<String> testSortedExpectedPartitionValues = expectedPartitionValueDaoTestHelper.getTestSortedExpectedPartitionValues();
        int testExpectedPartitionValueIndex = 3;
        for (Integer offset : Arrays.asList(-2, 0, 2))
        {
            ExpectedPartitionValueInformation resultPartitionValueInformation = expectedPartitionValueService.getExpectedPartitionValue(
                new ExpectedPartitionValueKey(PARTITION_KEY_GROUP, testSortedExpectedPartitionValues.get(testExpectedPartitionValueIndex)), offset);

            // Validate the returned object.
            validateExpectedPartitionValueInformation(PARTITION_KEY_GROUP, testSortedExpectedPartitionValues.get(testExpectedPartitionValueIndex + offset),
                resultPartitionValueInformation);
        }
    }

    @Test
    public void testGetExpectedPartitionValueMissingRequiredParameters()
    {
        // Try to perform a get expected partition value without specifying partition key group name.
        try
        {
            expectedPartitionValueService.getExpectedPartitionValue(new ExpectedPartitionValueKey(BLANK_TEXT, PARTITION_VALUE), 0);
            fail("Should throw an IllegalArgumentException when partition key group is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A partition key group name must be specified.", e.getMessage());
        }

        // Try to perform a get expected partition value without specifying the expected partition value.
        try
        {
            expectedPartitionValueService.getExpectedPartitionValue(new ExpectedPartitionValueKey(PARTITION_KEY_GROUP, BLANK_TEXT), 0);
            fail("Should throw an IllegalArgumentException when expected partition value is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("An expected partition value must be specified.", e.getMessage());
        }
    }

    @Test
    public void testGetExpectedPartitionValueMissingOptionalParameters()
    {
        // Create and persist a partition key group entity.
        PartitionKeyGroupEntity partitionKeyGroupEntity = partitionKeyGroupDaoTestHelper.createPartitionKeyGroupEntity(PARTITION_KEY_GROUP);

        // Create and persist a list of test expected partition values.
        expectedPartitionValueDaoTestHelper
            .createExpectedPartitionValueEntities(partitionKeyGroupEntity, expectedPartitionValueDaoTestHelper.getTestUnsortedExpectedPartitionValues());

        // Get expected partition value without specifying the offset.
        List<String> testSortedExpectedPartitionValues = expectedPartitionValueDaoTestHelper.getTestSortedExpectedPartitionValues();
        int testExpectedPartitionValueIndex = 3;
        ExpectedPartitionValueInformation resultPartitionValueInformation = expectedPartitionValueService.getExpectedPartitionValue(
            new ExpectedPartitionValueKey(PARTITION_KEY_GROUP, testSortedExpectedPartitionValues.get(testExpectedPartitionValueIndex)), null);

        // Validate the returned object.
        validateExpectedPartitionValueInformation(PARTITION_KEY_GROUP, testSortedExpectedPartitionValues.get(testExpectedPartitionValueIndex),
            resultPartitionValueInformation);
    }

    @Test
    public void testGetExpectedPartitionValueTrimParameters()
    {
        // Create and persist a partition key group entity.
        PartitionKeyGroupEntity partitionKeyGroupEntity = partitionKeyGroupDaoTestHelper.createPartitionKeyGroupEntity(PARTITION_KEY_GROUP);

        // Create and persist a list of test expected partition values.
        expectedPartitionValueDaoTestHelper
            .createExpectedPartitionValueEntities(partitionKeyGroupEntity, expectedPartitionValueDaoTestHelper.getTestUnsortedExpectedPartitionValues());

        // Get expected partition value with offset set to 0 and with request parameters padded with whitespace characters.
        List<String> testSortedExpectedPartitionValues = expectedPartitionValueDaoTestHelper.getTestSortedExpectedPartitionValues();
        int testExpectedPartitionValueIndex = 3;
        ExpectedPartitionValueInformation resultPartitionValueInformation = expectedPartitionValueService.getExpectedPartitionValue(
            new ExpectedPartitionValueKey(addWhitespace(PARTITION_KEY_GROUP),
                addWhitespace(testSortedExpectedPartitionValues.get(testExpectedPartitionValueIndex))), 0);

        // Validate the returned object.
        validateExpectedPartitionValueInformation(PARTITION_KEY_GROUP, testSortedExpectedPartitionValues.get(testExpectedPartitionValueIndex),
            resultPartitionValueInformation);
    }

    @Test
    public void testGetExpectedPartitionValueUpperCaseParameters()
    {
        // Create and persist a partition key group entity.
        PartitionKeyGroupEntity partitionKeyGroupEntity = partitionKeyGroupDaoTestHelper.createPartitionKeyGroupEntity(PARTITION_KEY_GROUP.toLowerCase());

        // Create and persist a single test expected partition value.
        expectedPartitionValueDaoTestHelper.createExpectedPartitionValueEntities(partitionKeyGroupEntity, Arrays.asList(PARTITION_VALUE.toLowerCase()));

        // Get expected partition value with offset set to null using relative input parameters in upper case.
        ExpectedPartitionValueInformation resultPartitionValueInformation = expectedPartitionValueService
            .getExpectedPartitionValue(new ExpectedPartitionValueKey(PARTITION_KEY_GROUP.toUpperCase(), PARTITION_VALUE.toLowerCase()), null);

        // Validate the returned object.
        validateExpectedPartitionValueInformation(PARTITION_KEY_GROUP.toLowerCase(), PARTITION_VALUE.toLowerCase(), resultPartitionValueInformation);
    }

    @Test
    public void testGetExpectedPartitionValueLowerCaseParameters()
    {
        // Create and persist a partition key group entity.
        PartitionKeyGroupEntity partitionKeyGroupEntity = partitionKeyGroupDaoTestHelper.createPartitionKeyGroupEntity(PARTITION_KEY_GROUP.toUpperCase());

        // Create and persist a single test expected partition value.
        expectedPartitionValueDaoTestHelper.createExpectedPartitionValueEntities(partitionKeyGroupEntity, Arrays.asList(PARTITION_VALUE.toUpperCase()));

        // Get expected partition value with offset set to null using relative input parameters in lower case.
        ExpectedPartitionValueInformation resultPartitionValueInformation = expectedPartitionValueService
            .getExpectedPartitionValue(new ExpectedPartitionValueKey(PARTITION_KEY_GROUP.toLowerCase(), PARTITION_VALUE.toUpperCase()), null);

        // Validate the returned object.
        validateExpectedPartitionValueInformation(PARTITION_KEY_GROUP.toUpperCase(), PARTITION_VALUE.toUpperCase(), resultPartitionValueInformation);
    }

    @Test
    public void testGetExpectedPartitionValuePartitionKeyGroupNoExists()
    {
        // Try to perform a get expected partition value using a non-existing partition key group name.
        String partitionKeyGroupName = "I_DO_NOT_EXIST";
        try
        {
            expectedPartitionValueService.getExpectedPartitionValue(new ExpectedPartitionValueKey(partitionKeyGroupName, PARTITION_VALUE), null);
            fail("Should throw an IllegalArgumentException when partition key group does not exist.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Partition key group \"%s\" doesn't exist.", partitionKeyGroupName), e.getMessage());
        }
    }

    @Test
    public void testGetExpectedPartitionValueStartExpectedPartitionValueNoExists()
    {
        // Create and persist a partition key group entity.
        partitionKeyGroupDaoTestHelper.createPartitionKeyGroupEntity(PARTITION_KEY_GROUP);

        // Try to get an expected partition value by passing non-existing expected partition value with or without an offset.
        String testExpectedPartitionValue = "I_DO_NOT_EXIST";
        for (Integer offset : Arrays.asList(-2, 0, 2))
        {
            try
            {
                expectedPartitionValueService.getExpectedPartitionValue(new ExpectedPartitionValueKey(PARTITION_KEY_GROUP, testExpectedPartitionValue), offset);
                fail("Should throw an IllegalArgumentException when the expected partition value does not exist.");
            }
            catch (ObjectNotFoundException e)
            {
                assertEquals(String
                    .format("Expected partition value \"%s\" doesn't exist in \"%s\" partition key group.", testExpectedPartitionValue, PARTITION_KEY_GROUP),
                    e.getMessage());
            }
        }
    }

    @Test
    public void testGetExpectedPartitionValueOffsetExpectedPartitionValueNoExists()
    {
        // Create and persist a partition key group entity.
        PartitionKeyGroupEntity partitionKeyGroupEntity = partitionKeyGroupDaoTestHelper.createPartitionKeyGroupEntity(PARTITION_KEY_GROUP);

        // Create and persist a single test expected partition value.
        expectedPartitionValueDaoTestHelper.createExpectedPartitionValueEntities(partitionKeyGroupEntity, Arrays.asList(PARTITION_VALUE));

        // Try to get a non-existing expected partition value by passing an existing expected partition value but giving an invalid offset.
        for (Integer offset : Arrays.asList(-1, 1))
        {
            try
            {
                expectedPartitionValueService.getExpectedPartitionValue(new ExpectedPartitionValueKey(PARTITION_KEY_GROUP, PARTITION_VALUE), offset);
                fail("Should throw an IllegalArgumentException when the expected partition value does not exist.");
            }
            catch (ObjectNotFoundException e)
            {
                assertEquals(String
                    .format("Expected partition value \"%s\" with offset %d doesn't exist in \"%s\" partition key group.", PARTITION_VALUE, offset,
                        PARTITION_KEY_GROUP), e.getMessage());
            }
        }
    }

    @Test
    public void testGetExpectedPartitionValues()
    {
        // Create and persist a partition key group entity.
        PartitionKeyGroupEntity partitionKeyGroupEntity = partitionKeyGroupDaoTestHelper.createPartitionKeyGroupEntity(PARTITION_KEY_GROUP);

        // Create and persist a list of test expected partition values.
        expectedPartitionValueDaoTestHelper
            .createExpectedPartitionValueEntities(partitionKeyGroupEntity, expectedPartitionValueDaoTestHelper.getTestUnsortedExpectedPartitionValues());

        // Get expected partition values for a range.
        List<String> testSortedExpectedPartitionValues = expectedPartitionValueDaoTestHelper.getTestSortedExpectedPartitionValues();
        int startExpectedPartitionValueIndex = 1;
        int endExpectedPartitionValueIndex = testSortedExpectedPartitionValues.size() - 2;
        ExpectedPartitionValuesInformation resultPartitionValuesInformation = expectedPartitionValueService
            .getExpectedPartitionValues(new PartitionKeyGroupKey(PARTITION_KEY_GROUP),
                new PartitionValueRange(testSortedExpectedPartitionValues.get(startExpectedPartitionValueIndex),
                    testSortedExpectedPartitionValues.get(endExpectedPartitionValueIndex)));

        // Validate the returned object.
        validateExpectedPartitionValuesInformation(PARTITION_KEY_GROUP,
            testSortedExpectedPartitionValues.subList(startExpectedPartitionValueIndex, endExpectedPartitionValueIndex + 1), resultPartitionValuesInformation);
    }

    @Test
    public void testGetExpectedPartitionValuesMissingRequiredParameters()
    {
        // Try to perform a get expected partition values without specifying partition key group name.
        try
        {
            expectedPartitionValueService
                .getExpectedPartitionValues(new PartitionKeyGroupKey(BLANK_TEXT), new PartitionValueRange(PARTITION_VALUE, PARTITION_VALUE));
            fail("Should throw an IllegalArgumentException when partition key group is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A partition key group name must be specified.", e.getMessage());
        }

        // Try to perform a get expected partition values without specifying neither start or end expected partition values for the range.
        try
        {
            expectedPartitionValueService
                .getExpectedPartitionValues(new PartitionKeyGroupKey(PARTITION_KEY_GROUP), new PartitionValueRange(BLANK_TEXT, BLANK_TEXT));
            fail("Should throw an IllegalArgumentException when both start and end expected partition values are not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("At least one start or end expected partition value must be specified.", e.getMessage());
        }
    }

    @Test
    public void testGetExpectedPartitionValuesMissingOptionalParameters()
    {
        // Create and persist a partition key group entity.
        PartitionKeyGroupEntity partitionKeyGroupEntity = partitionKeyGroupDaoTestHelper.createPartitionKeyGroupEntity(PARTITION_KEY_GROUP);

        // Create and persist a list of test expected partition values.
        expectedPartitionValueDaoTestHelper
            .createExpectedPartitionValueEntities(partitionKeyGroupEntity, expectedPartitionValueDaoTestHelper.getTestUnsortedExpectedPartitionValues());

        // Get the sorted list of test expected partition values.
        List<String> testSortedExpectedPartitionValues = expectedPartitionValueDaoTestHelper.getTestSortedExpectedPartitionValues();
        int startExpectedPartitionValueIndex = 1;
        int endExpectedPartitionValueIndex = testSortedExpectedPartitionValues.size() - 2;

        ExpectedPartitionValuesInformation resultPartitionValuesInformation;

        // Get expected partition values for a range without specifying the end expected partition value.
        resultPartitionValuesInformation = expectedPartitionValueService.getExpectedPartitionValues(new PartitionKeyGroupKey(PARTITION_KEY_GROUP),
            new PartitionValueRange(testSortedExpectedPartitionValues.get(startExpectedPartitionValueIndex), BLANK_TEXT));

        // Validate the returned object.
        validateExpectedPartitionValuesInformation(PARTITION_KEY_GROUP, testSortedExpectedPartitionValues
            .subList(startExpectedPartitionValueIndex, expectedPartitionValueDaoTestHelper.getTestSortedExpectedPartitionValues().size()),
            resultPartitionValuesInformation);

        // Get expected partition values for a range without specifying the start expected partition value.
        resultPartitionValuesInformation = expectedPartitionValueService.getExpectedPartitionValues(new PartitionKeyGroupKey(PARTITION_KEY_GROUP),
            new PartitionValueRange(BLANK_TEXT, testSortedExpectedPartitionValues.get(endExpectedPartitionValueIndex)));

        // Validate the returned object.
        validateExpectedPartitionValuesInformation(PARTITION_KEY_GROUP, testSortedExpectedPartitionValues.subList(0, endExpectedPartitionValueIndex + 1),
            resultPartitionValuesInformation);
    }

    @Test
    public void testGetExpectedPartitionValuesTrimParameters()
    {
        // Create and persist a partition key group entity.
        PartitionKeyGroupEntity partitionKeyGroupEntity = partitionKeyGroupDaoTestHelper.createPartitionKeyGroupEntity(PARTITION_KEY_GROUP);

        // Create and persist a list of test expected partition values.
        expectedPartitionValueDaoTestHelper
            .createExpectedPartitionValueEntities(partitionKeyGroupEntity, expectedPartitionValueDaoTestHelper.getTestUnsortedExpectedPartitionValues());

        // Get expected partition values for a range with request parameters padded with whitespace characters.
        List<String> testSortedExpectedPartitionValues = expectedPartitionValueDaoTestHelper.getTestSortedExpectedPartitionValues();
        ExpectedPartitionValuesInformation resultPartitionValuesInformation = expectedPartitionValueService
            .getExpectedPartitionValues(new PartitionKeyGroupKey(addWhitespace(PARTITION_KEY_GROUP)),
                new PartitionValueRange(addWhitespace(testSortedExpectedPartitionValues.get(0)),
                    addWhitespace(testSortedExpectedPartitionValues.get(testSortedExpectedPartitionValues.size() - 1))));

        // Validate the returned object.
        validateExpectedPartitionValuesInformation(PARTITION_KEY_GROUP, testSortedExpectedPartitionValues, resultPartitionValuesInformation);
    }

    @Test
    public void testGetExpectedPartitionValuesUpperCaseParameters()
    {
        // Create and persist a partition key group entity.
        PartitionKeyGroupEntity partitionKeyGroupEntity = partitionKeyGroupDaoTestHelper.createPartitionKeyGroupEntity(PARTITION_KEY_GROUP.toLowerCase());

        // Create and persist a single test expected partition value.
        expectedPartitionValueDaoTestHelper.createExpectedPartitionValueEntities(partitionKeyGroupEntity, Arrays.asList(PARTITION_VALUE.toLowerCase()));

        // Get expected partition values for a range using relative input parameters in upper case.
        ExpectedPartitionValuesInformation resultPartitionValuesInformation = expectedPartitionValueService
            .getExpectedPartitionValues(new PartitionKeyGroupKey(PARTITION_KEY_GROUP.toUpperCase()),
                new PartitionValueRange(PARTITION_VALUE.toLowerCase(), PARTITION_VALUE.toLowerCase()));

        // Validate the returned object.
        validateExpectedPartitionValuesInformation(PARTITION_KEY_GROUP.toLowerCase(), Arrays.asList(PARTITION_VALUE.toLowerCase()),
            resultPartitionValuesInformation);
    }

    @Test
    public void testGetExpectedPartitionValuesLowerCaseParameters()
    {
        // Create and persist a partition key group entity.
        PartitionKeyGroupEntity partitionKeyGroupEntity = partitionKeyGroupDaoTestHelper.createPartitionKeyGroupEntity(PARTITION_KEY_GROUP.toUpperCase());

        // Create and persist a single test expected partition value.
        expectedPartitionValueDaoTestHelper.createExpectedPartitionValueEntities(partitionKeyGroupEntity, Arrays.asList(PARTITION_VALUE.toUpperCase()));

        // Get expected partition values for a range using relative input parameters in lower case.
        ExpectedPartitionValuesInformation resultPartitionValuesInformation = expectedPartitionValueService
            .getExpectedPartitionValues(new PartitionKeyGroupKey(PARTITION_KEY_GROUP.toLowerCase()),
                new PartitionValueRange(PARTITION_VALUE.toUpperCase(), PARTITION_VALUE.toUpperCase()));

        // Validate the returned object.
        validateExpectedPartitionValuesInformation(PARTITION_KEY_GROUP.toUpperCase(), Arrays.asList(PARTITION_VALUE.toUpperCase()),
            resultPartitionValuesInformation);
    }

    @Test
    public void testGetExpectedPartitionValuesInvalidParameters()
    {
        // Try to perform a get expected partition values with the start expected partition value being greater than the end expected partition value.
        try
        {
            expectedPartitionValueService
                .getExpectedPartitionValues(new PartitionKeyGroupKey(PARTITION_KEY_GROUP), new PartitionValueRange(PARTITION_VALUE_2, PARTITION_VALUE));
            fail("Should throw an IllegalArgumentException when the start expected partition value being greater than the end expected partition value.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String
                .format("The start expected partition value \"%s\" cannot be greater than the end expected partition value \"%s\".", PARTITION_VALUE_2,
                    PARTITION_VALUE), e.getMessage());
        }
    }

    @Test
    public void testGetExpectedPartitionValuesPartitionKeyGroupNoExists()
    {
        // Try to perform a get expected partition values using a non-existing partition key group name.
        String partitionKeyGroupName = "I_DO_NOT_EXIST";
        try
        {
            expectedPartitionValueService
                .getExpectedPartitionValues(new PartitionKeyGroupKey(partitionKeyGroupName), new PartitionValueRange(PARTITION_VALUE, PARTITION_VALUE));
            fail("Should throw an IllegalArgumentException when partition key group does not exist.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Partition key group \"%s\" doesn't exist.", partitionKeyGroupName), e.getMessage());
        }
    }

    @Test
    public void testGetExpectedPartitionValuesExpectedPartitionValuesNoExist()
    {
        // Create and persist a partition key group entity.
        partitionKeyGroupDaoTestHelper.createPartitionKeyGroupEntity(PARTITION_KEY_GROUP);

        // Get a range of non-existing expected partition values.
        ExpectedPartitionValuesInformation resultPartitionValuesInformation = expectedPartitionValueService
            .getExpectedPartitionValues(new PartitionKeyGroupKey(PARTITION_KEY_GROUP), new PartitionValueRange(PARTITION_VALUE, PARTITION_VALUE_2));

        // Validate that returned object contains an empty list of expected partition values.
        validateExpectedPartitionValuesInformation(PARTITION_KEY_GROUP, new ArrayList<String>(), resultPartitionValuesInformation);
    }

    @Test
    public void testDeleteExpectedPartitionValues()
    {
        // Create and persist a partition key group entity.
        PartitionKeyGroupEntity partitionKeyGroupEntity = partitionKeyGroupDaoTestHelper.createPartitionKeyGroupEntity(PARTITION_KEY_GROUP);

        // Create and persist a list of test expected partition values.
        expectedPartitionValueDaoTestHelper
            .createExpectedPartitionValueEntities(partitionKeyGroupEntity, expectedPartitionValueDaoTestHelper.getTestUnsortedExpectedPartitionValues());

        // Delete expected partition values from this partition key group.
        ExpectedPartitionValuesDeleteRequest request =
            createExpectedPartitionValuesDeleteRequest(PARTITION_KEY_GROUP, expectedPartitionValueDaoTestHelper.getTestUnsortedExpectedPartitionValues());
        ExpectedPartitionValuesInformation resultPartitionValuesInformation = expectedPartitionValueService.deleteExpectedPartitionValues(request);

        // Validate the returned object.
        validateExpectedPartitionValuesInformation(PARTITION_KEY_GROUP, expectedPartitionValueDaoTestHelper.getTestSortedExpectedPartitionValues(),
            resultPartitionValuesInformation);

        // Validate that the expected partition value entities got deleted.
        assertEquals(0, partitionKeyGroupEntity.getExpectedPartitionValues().size());
    }

    @Test
    public void testDeleteExpectedPartitionValuesMissingRequiredParameters()
    {
        ExpectedPartitionValuesDeleteRequest request;

        // Try to perform a delete without specifying partition key group name.
        request = createExpectedPartitionValuesDeleteRequest(BLANK_TEXT, expectedPartitionValueDaoTestHelper.getTestUnsortedExpectedPartitionValues());
        try
        {
            expectedPartitionValueService.deleteExpectedPartitionValues(request);
            fail("Should throw an IllegalArgumentException when partition key group is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A partition key group name must be specified.", e.getMessage());
        }

        // Try to perform a delete without specifying any expected partition values.
        request = createExpectedPartitionValuesDeleteRequest(PARTITION_KEY_GROUP, new ArrayList<String>());
        try
        {
            expectedPartitionValueService.deleteExpectedPartitionValues(request);
            fail("Should throw an IllegalArgumentException when no expected partition values are specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("At least one expected partition value must be specified.", e.getMessage());
        }

        // Try to perform a delete with a missing expected partition value.
        request = createExpectedPartitionValuesDeleteRequest(PARTITION_KEY_GROUP, Arrays.asList(BLANK_TEXT));
        try
        {
            expectedPartitionValueService.deleteExpectedPartitionValues(request);
            fail("Should throw an IllegalArgumentException when expected partition value is missing.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("An expected partition value must be specified.", e.getMessage());
        }
    }

    @Test
    public void testDeleteExpectedPartitionValuesTrimParameters()
    {
        // Create and persist a partition key group entity.
        PartitionKeyGroupEntity partitionKeyGroupEntity = partitionKeyGroupDaoTestHelper.createPartitionKeyGroupEntity(PARTITION_KEY_GROUP);

        // Create and persist a list of test expected partition values.
        expectedPartitionValueDaoTestHelper
            .createExpectedPartitionValueEntities(partitionKeyGroupEntity, expectedPartitionValueDaoTestHelper.getTestUnsortedExpectedPartitionValues());

        // Delete expected partition values from this partition key group with request parameters padded with whitespace characters.
        ExpectedPartitionValuesDeleteRequest request = createExpectedPartitionValuesDeleteRequest(addWhitespace(PARTITION_KEY_GROUP),
            expectedPartitionValueDaoTestHelper.getTestUnsortedExpectedPartitionValues());
        for (int i = 0; i < request.getExpectedPartitionValues().size(); i++)
        {
            request.getExpectedPartitionValues().set(i, addWhitespace(request.getExpectedPartitionValues().get(i)));
        }
        ExpectedPartitionValuesInformation resultPartitionValuesInformation = expectedPartitionValueService.deleteExpectedPartitionValues(request);

        // Validate the returned object.
        validateExpectedPartitionValuesInformation(PARTITION_KEY_GROUP, expectedPartitionValueDaoTestHelper.getTestSortedExpectedPartitionValues(),
            resultPartitionValuesInformation);

        // Validate that the expected partition value entities got deleted.
        assertEquals(0, partitionKeyGroupEntity.getExpectedPartitionValues().size());
    }

    @Test
    public void testDeleteExpectedPartitionValuesDuplicatePartitionValues()
    {
        // Try to perform a delete by passing duplicate expected partition values.
        ExpectedPartitionValuesDeleteRequest request =
            createExpectedPartitionValuesDeleteRequest(PARTITION_KEY_GROUP, Arrays.asList(PARTITION_VALUE, PARTITION_VALUE));
        try
        {
            expectedPartitionValueService.deleteExpectedPartitionValues(request);
            fail("Should throw an IllegalArgumentException when delete request contains duplicate expected partition values.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Duplicate expected partition value \"%s\" found.", PARTITION_VALUE), e.getMessage());
        }
    }

    @Test
    public void testDeleteExpectedPartitionValuesPartitionKeyGroupNoExists()
    {
        // Try to perform a delete using a non-existing partition key group name.
        ExpectedPartitionValuesDeleteRequest request =
            createExpectedPartitionValuesDeleteRequest("I_DO_NOT_EXIST", expectedPartitionValueDaoTestHelper.getTestUnsortedExpectedPartitionValues());
        try
        {
            expectedPartitionValueService.deleteExpectedPartitionValues(request);
            fail("Should throw an IllegalArgumentException when partition key group does not exist.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Partition key group \"%s\" doesn't exist.", request.getPartitionKeyGroupKey().getPartitionKeyGroupName()),
                e.getMessage());
        }
    }

    @Test
    public void testDeleteExpectedPartitionValuesExpectedPartitionValueNoExists()
    {
        // Create and persist a partition key group entity.
        partitionKeyGroupDaoTestHelper.createPartitionKeyGroupEntity(PARTITION_KEY_GROUP);

        // Try to delete a non-existing expected partition value.
        ExpectedPartitionValuesDeleteRequest request = createExpectedPartitionValuesDeleteRequest(PARTITION_KEY_GROUP, Arrays.asList("I_DO_NOT_EXIST"));
        try
        {
            expectedPartitionValueService.deleteExpectedPartitionValues(request);
            fail("Should throw an IllegalArgumentException when any of the expected partition values do not exist.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String
                .format("Expected partition value \"%s\" doesn't exist in \"%s\" partition key group.", request.getExpectedPartitionValues().get(0),
                    PARTITION_KEY_GROUP), e.getMessage());
        }
    }

    @Test
    public void testLargeNumberOfExpectedPartitionValues()
    {
        // Define some constants.
        final int MAX_PARTITION_VALUES = 1000;
        final int LAST_ELEMENT_INDEX = MAX_PARTITION_VALUES - 1;

        // Create and persist a partition key group entity.
        PartitionKeyGroupEntity partitionKeyGroupEntity = partitionKeyGroupDaoTestHelper.createPartitionKeyGroupEntity(PARTITION_KEY_GROUP);

        // Add expected partition values to this partition key group.
        List<String> testUnsortedExpectedPartitionValues = expectedPartitionValueDaoTestHelper.getTestUnsortedExpectedPartitionValues(MAX_PARTITION_VALUES);
        List<String> testSortedExpectedPartitionValues = expectedPartitionValueDaoTestHelper.getTestSortedExpectedPartitionValues(MAX_PARTITION_VALUES);
        ExpectedPartitionValuesCreateRequest createRequest =
            createExpectedPartitionValuesCreateRequest(PARTITION_KEY_GROUP, testUnsortedExpectedPartitionValues);
        ExpectedPartitionValuesInformation resultPartitionValuesInformation = expectedPartitionValueService.createExpectedPartitionValues(createRequest);

        // Validate the returned object.
        validateExpectedPartitionValuesInformation(PARTITION_KEY_GROUP, testSortedExpectedPartitionValues, resultPartitionValuesInformation);

        // Validate that the expected partition value entities got created.
        assertEquals(testUnsortedExpectedPartitionValues.size(), partitionKeyGroupEntity.getExpectedPartitionValues().size());

        // Get expected partition value without an offset.
        ExpectedPartitionValueInformation resultPartitionValueInformation = expectedPartitionValueService
            .getExpectedPartitionValue(new ExpectedPartitionValueKey(PARTITION_KEY_GROUP, testSortedExpectedPartitionValues.get(MAX_PARTITION_VALUES / 2)),
                null);

        // Validate the returned object.
        validateExpectedPartitionValueInformation(PARTITION_KEY_GROUP, testSortedExpectedPartitionValues.get(MAX_PARTITION_VALUES / 2),
            resultPartitionValueInformation);

        // Get expected partition value by passing a large positive offset.
        resultPartitionValueInformation = expectedPartitionValueService
            .getExpectedPartitionValue(new ExpectedPartitionValueKey(PARTITION_KEY_GROUP, testSortedExpectedPartitionValues.get(0)), LAST_ELEMENT_INDEX);

        // Validate the returned object.
        validateExpectedPartitionValueInformation(PARTITION_KEY_GROUP, testSortedExpectedPartitionValues.get(LAST_ELEMENT_INDEX),
            resultPartitionValueInformation);

        // Get expected partition value by passing a large negative offset.
        resultPartitionValueInformation = expectedPartitionValueService
            .getExpectedPartitionValue(new ExpectedPartitionValueKey(PARTITION_KEY_GROUP, testSortedExpectedPartitionValues.get(LAST_ELEMENT_INDEX)),
                -LAST_ELEMENT_INDEX);

        // Validate the returned object.
        validateExpectedPartitionValueInformation(PARTITION_KEY_GROUP, testSortedExpectedPartitionValues.get(0), resultPartitionValueInformation);

        // Get a range of expected partition values.
        resultPartitionValuesInformation = expectedPartitionValueService.getExpectedPartitionValues(new PartitionKeyGroupKey(PARTITION_KEY_GROUP),
            new PartitionValueRange(testSortedExpectedPartitionValues.get(0), testSortedExpectedPartitionValues.get(LAST_ELEMENT_INDEX)));

        // Validate the returned object.
        validateExpectedPartitionValuesInformation(PARTITION_KEY_GROUP, testSortedExpectedPartitionValues, resultPartitionValuesInformation);

        // Delete expected partition values from this partition key group.
        ExpectedPartitionValuesDeleteRequest deleteRequest =
            createExpectedPartitionValuesDeleteRequest(PARTITION_KEY_GROUP, testUnsortedExpectedPartitionValues);
        ExpectedPartitionValuesInformation deleteResultPartitionValuesInformation = expectedPartitionValueService.deleteExpectedPartitionValues(deleteRequest);

        // Validate the returned object.
        validateExpectedPartitionValuesInformation(PARTITION_KEY_GROUP, testSortedExpectedPartitionValues, deleteResultPartitionValuesInformation);

        // Validate that the expected partition value entities got deleted.
        assertEquals(0, partitionKeyGroupEntity.getExpectedPartitionValues().size());
    }

    /**
     * This method is to get coverage for all expected partition value service methods that start a new transaction.
     */
    @Test
    public void testExpectedPartitionValueServiceMethodsNewTx()
    {
        try
        {
            expectedPartitionValueServiceImpl.getExpectedPartitionValue(null, 0);
            fail("Should throw an IllegalArgumentException.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("An expected partition value key must be specified.", e.getMessage());
        }
    }
}
