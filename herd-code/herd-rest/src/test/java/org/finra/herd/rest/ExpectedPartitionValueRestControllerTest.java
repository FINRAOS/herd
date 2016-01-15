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
package org.finra.herd.rest;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import org.finra.herd.model.jpa.PartitionKeyGroupEntity;
import org.finra.herd.model.api.xml.ExpectedPartitionValueInformation;
import org.finra.herd.model.api.xml.ExpectedPartitionValuesCreateRequest;
import org.finra.herd.model.api.xml.ExpectedPartitionValuesDeleteRequest;
import org.finra.herd.model.api.xml.ExpectedPartitionValuesInformation;

/**
 * This class tests various functionality within the expected partition value REST controller.
 */
public class ExpectedPartitionValueRestControllerTest extends AbstractRestTest
{
    @Test
    public void testCreateExpectedPartitionValues()
    {
        // Create and persist a partition key group entity.
        createPartitionKeyGroupEntity(PARTITION_KEY_GROUP);

        // Add expected partition values to this partition key group.
        ExpectedPartitionValuesCreateRequest request =
            createExpectedPartitionValuesCreateRequest(PARTITION_KEY_GROUP, getTestUnsortedExpectedPartitionValues());
        ExpectedPartitionValuesInformation resultPartitionValuesInformation = expectedPartitionValueRestController.createExpectedPartitionValues(request);

        // Validate the returned object.
        validateExpectedPartitionValuesInformation(PARTITION_KEY_GROUP, getTestSortedExpectedPartitionValues(), resultPartitionValuesInformation);
    }

    @Test
    public void testGetExpectedPartitionValue()
    {
        // Create and persist a partition key group entity.
        PartitionKeyGroupEntity partitionKeyGroupEntity = createPartitionKeyGroupEntity(PARTITION_KEY_GROUP);

        // Create and persist a list of test expected partition values.
        createExpectedPartitionValueEntities(partitionKeyGroupEntity, getTestUnsortedExpectedPartitionValues());

        // Get expected partition value for different offset values.
        List<String> testSortedExpectedPartitionValues = getTestSortedExpectedPartitionValues();
        int testExpectedPartitionValueIndex = 3;
        for (Integer offset : Arrays.asList(-2, 0, 2))
        {
            ExpectedPartitionValueInformation resultPartitionValueInformation = expectedPartitionValueRestController
                .getExpectedPartitionValue(PARTITION_KEY_GROUP, testSortedExpectedPartitionValues.get(testExpectedPartitionValueIndex), offset);

            // Validate the returned object.
            validateExpectedPartitionValueInformation(PARTITION_KEY_GROUP, testSortedExpectedPartitionValues.get(testExpectedPartitionValueIndex + offset),
                resultPartitionValueInformation);
        }
    }

    @Test
    public void testGetExpectedPartitionValues()
    {
        // Create and persist a partition key group entity.
        PartitionKeyGroupEntity partitionKeyGroupEntity = createPartitionKeyGroupEntity(PARTITION_KEY_GROUP);

        // Create and persist a list of test expected partition values.
        createExpectedPartitionValueEntities(partitionKeyGroupEntity, getTestUnsortedExpectedPartitionValues());

        // Get expected partition values for a range.
        List<String> testSortedExpectedPartitionValues = getTestSortedExpectedPartitionValues();
        int startExpectedPartitionValueIndex = 1;
        int endExpectedPartitionValueIndex = testSortedExpectedPartitionValues.size() - 2;
        ExpectedPartitionValuesInformation resultPartitionValuesInformation = expectedPartitionValueRestController
            .getExpectedPartitionValues(PARTITION_KEY_GROUP, testSortedExpectedPartitionValues.get(startExpectedPartitionValueIndex),
                testSortedExpectedPartitionValues.get(endExpectedPartitionValueIndex));

        // Validate the returned object.
        validateExpectedPartitionValuesInformation(PARTITION_KEY_GROUP,
            testSortedExpectedPartitionValues.subList(startExpectedPartitionValueIndex, endExpectedPartitionValueIndex + 1), resultPartitionValuesInformation);
    }

    @Test
    public void testDeleteExpectedPartitionValues()
    {
        // Create and persist a partition key group entity.
        PartitionKeyGroupEntity partitionKeyGroupEntity = createPartitionKeyGroupEntity(PARTITION_KEY_GROUP);

        // Create and persist a list of test expected partition values.
        createExpectedPartitionValueEntities(partitionKeyGroupEntity, getTestUnsortedExpectedPartitionValues());

        // Delete expected partition values from this partition key group.
        ExpectedPartitionValuesDeleteRequest request =
            createExpectedPartitionValuesDeleteRequest(PARTITION_KEY_GROUP, getTestUnsortedExpectedPartitionValues());
        ExpectedPartitionValuesInformation resultPartitionValuesInformation = expectedPartitionValueRestController.deleteExpectedPartitionValues(request);

        // Validate the returned object.
        validateExpectedPartitionValuesInformation(PARTITION_KEY_GROUP, getTestSortedExpectedPartitionValues(), resultPartitionValuesInformation);

        // Validate that the expected partition value entities got deleted.
        assertEquals(0, partitionKeyGroupEntity.getExpectedPartitionValues().size());
    }
}
