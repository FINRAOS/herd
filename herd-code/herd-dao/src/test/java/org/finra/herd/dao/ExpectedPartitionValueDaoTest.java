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
package org.finra.herd.dao;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.GregorianCalendar;
import java.util.List;

import org.junit.Test;

import org.finra.herd.dao.impl.AbstractHerdDao;
import org.finra.herd.model.api.xml.ExpectedPartitionValueKey;
import org.finra.herd.model.api.xml.PartitionValueRange;
import org.finra.herd.model.jpa.ExpectedPartitionValueEntity;
import org.finra.herd.model.jpa.PartitionKeyGroupEntity;

public class ExpectedPartitionValueDaoTest extends AbstractDaoTest
{
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
            ExpectedPartitionValueEntity resultExpectedPartitionValueEntity = expectedPartitionValueDao.getExpectedPartitionValue(new ExpectedPartitionValueKey(
                PARTITION_KEY_GROUP, testSortedExpectedPartitionValues.get(testExpectedPartitionValueIndex)), offset);

            // Validate the returned object.
            resultExpectedPartitionValueEntity.getPartitionValue().equals(testSortedExpectedPartitionValues.get(testExpectedPartitionValueIndex + offset));
        }
    }

    @Test
    public void testGetExpectedPartitionValueWithOffsetExpectedPartitionValueNoExists()
    {
        // Create and persist a partition key group entity.
        PartitionKeyGroupEntity partitionKeyGroupEntity = createPartitionKeyGroupEntity(PARTITION_KEY_GROUP);

        // Create and persist a single test expected partition value.
        createExpectedPartitionValueEntities(partitionKeyGroupEntity, Arrays.asList(PARTITION_VALUE));

        // Validate that we get null back when passing an existing expected partition value but giving an invalid offset.
        for (Integer offset : Arrays.asList(-1, 1))
        {
            assertNull(expectedPartitionValueDao.getExpectedPartitionValue(new ExpectedPartitionValueKey(PARTITION_KEY_GROUP, PARTITION_VALUE), offset));
        }
    }

    /**
     * Test DAO method to retrieve expected partition values by range.
     */
    @Test
    public void testGetExpectedPartitionValuesByGroupAndRange()
    {
        createExpectedPartitionValueProcessDatesForApril2014(PARTITION_KEY_GROUP);

        PartitionValueRange partitionValueRange = new PartitionValueRange();
        partitionValueRange.setStartPartitionValue(getDateAsString(2014, 3, 11));
        partitionValueRange.setEndPartitionValue(getDateAsString(2014, 3, 17));
        List<ExpectedPartitionValueEntity> expectedPartitionValueEntities = expectedPartitionValueDao.getExpectedPartitionValuesByGroupAndRange(
            PARTITION_KEY_GROUP, partitionValueRange);

        assertEquals(expectedPartitionValueEntities.size(), 5, expectedPartitionValueEntities.size());
        assertEquals(expectedPartitionValueEntities.get(0).getPartitionValue(), getDateAsString(2014, 3, 11));
        assertEquals(expectedPartitionValueEntities.get(1).getPartitionValue(), getDateAsString(2014, 3, 14));
        assertEquals(expectedPartitionValueEntities.get(2).getPartitionValue(), getDateAsString(2014, 3, 15));
        assertEquals(expectedPartitionValueEntities.get(3).getPartitionValue(), getDateAsString(2014, 3, 16));
        assertEquals(expectedPartitionValueEntities.get(4).getPartitionValue(), getDateAsString(2014, 3, 17));
    }

    /**
     * Test DAO method to retrieve expected partition values with no range (specified 2 ways). In the month of April, 2014, the number of values (i.e.
     * non-weekend days) is 22.
     */
    @Test
    public void testGetExpectedPartitionValuesByGroupAndNoRange()
    {
        createExpectedPartitionValueProcessDatesForApril2014(PARTITION_KEY_GROUP);

        // Null range.
        List<ExpectedPartitionValueEntity> expectedPartitionValueEntities = expectedPartitionValueDao.getExpectedPartitionValuesByGroupAndRange(
            PARTITION_KEY_GROUP, null);

        assertEquals(expectedPartitionValueEntities.size(), 22, expectedPartitionValueEntities.size());

        // Range with no start or end.
        PartitionValueRange partitionValueRange = new PartitionValueRange();
        expectedPartitionValueEntities = expectedPartitionValueDao.getExpectedPartitionValuesByGroupAndRange(PARTITION_KEY_GROUP, partitionValueRange);

        assertEquals(expectedPartitionValueEntities.size(), 22, expectedPartitionValueEntities.size());
    }

    // Helper methods.

    /**
     * Gets a date as a string.
     *
     * @param year the year of the date.
     * @param month the month of the date. Note that month is 0-based as per GregorianCalendar.
     * @param day the day of the date.
     *
     * @return the date as a string in the format using HerdDao.DEFAULT_SINGLE_DAY_DATE_MASK.
     */
    private String getDateAsString(int year, int month, int day)
    {
        return new SimpleDateFormat(AbstractHerdDao.DEFAULT_SINGLE_DAY_DATE_MASK).format(new GregorianCalendar(year, month, day).getTime());
    }
}
