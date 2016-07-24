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

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.dao.impl.AbstractHerdDao;
import org.finra.herd.model.jpa.ExpectedPartitionValueEntity;
import org.finra.herd.model.jpa.PartitionKeyGroupEntity;

@Component
public class ExpectedPartitionValueDaoTestHelper
{
    @Autowired
    private ExpectedPartitionValueDao expectedPartitionValueDao;

    @Autowired
    private PartitionKeyGroupDao partitionKeyGroupDao;

    @Autowired
    private PartitionKeyGroupDaoTestHelper partitionKeyGroupDaoTestHelper;

    /**
     * Creates and persists specified partition value entities.  This method also creates and persists a partition key group entity, if it does not exist.
     *
     * @param partitionKeyGroupName the partition key group name
     * @param expectedPartitionValues the list of expected partition values
     *
     * @return the list of expected partition value entities
     */
    public List<ExpectedPartitionValueEntity> createExpectedPartitionValueEntities(String partitionKeyGroupName, List<String> expectedPartitionValues)
    {
        // Create partition key group if it does not exist.
        PartitionKeyGroupEntity partitionKeyGroupEntity = partitionKeyGroupDao.getPartitionKeyGroupByName(partitionKeyGroupName);
        if (partitionKeyGroupEntity == null)
        {
            partitionKeyGroupEntity = partitionKeyGroupDaoTestHelper.createPartitionKeyGroupEntity(partitionKeyGroupName);
        }

        // Initialize the return list.
        List<ExpectedPartitionValueEntity> expectedPartitionValueEntities = new ArrayList<>();

        // Keep incrementing the start date until it is greater than the end date, or until we have 1000 dates to protect against having too many dates or an
        // infinite loop in case the end date is before the start date.
        for (String expectedPartitionValue : expectedPartitionValues)
        {
            ExpectedPartitionValueEntity expectedPartitionValueEntity = new ExpectedPartitionValueEntity();
            expectedPartitionValueEntity.setPartitionKeyGroup(partitionKeyGroupEntity);
            expectedPartitionValueEntity.setPartitionValue(expectedPartitionValue);
            expectedPartitionValueEntities.add(expectedPartitionValueDao.saveAndRefresh(expectedPartitionValueEntity));
        }

        // Return the list of entities.
        return expectedPartitionValueEntities;
    }

    /**
     * Creates and persists expected partition value entities.
     *
     * @param partitionKeyGroupEntity the partition key group entity
     * @param expectedPartitionValues the list of expected partition value entities
     */
    public void createExpectedPartitionValueEntities(PartitionKeyGroupEntity partitionKeyGroupEntity, List<String> expectedPartitionValues)
    {
        for (String expectedPartitionValue : expectedPartitionValues)
        {
            ExpectedPartitionValueEntity expectedPartitionValueEntity = new ExpectedPartitionValueEntity();
            expectedPartitionValueEntity.setPartitionKeyGroup(partitionKeyGroupEntity);
            expectedPartitionValueEntity.setPartitionValue(expectedPartitionValue);
            expectedPartitionValueDao.saveAndRefresh(expectedPartitionValueEntity);
        }
        partitionKeyGroupDao.saveAndRefresh(partitionKeyGroupEntity);
        assertEquals(expectedPartitionValues.size(), partitionKeyGroupEntity.getExpectedPartitionValues().size());
    }

    /**
     * Creates a list of expected partition value process dates for a specified range. Weekends are excluded.
     *
     * @param partitionKeyGroupName the partition key group name
     * @param startDate the start date of the range
     * @param endDate the end date of the range
     *
     * @return the list of expected partition value process dates
     */
    public List<ExpectedPartitionValueEntity> createExpectedPartitionValueProcessDates(String partitionKeyGroupName, Calendar startDate, Calendar endDate)
    {
        // Initialize the list of expected partition values.
        List<String> expectedPartitionValues = new ArrayList<>();

        // Keep incrementing the start date until it is greater than the end date, or until we have 1000 dates to protect against having too many dates or an
        // infinite loop in case the end date is before the start date.
        for (int i = 0; i < 1000 && startDate.compareTo(endDate) <= 0; i++)
        {
            // Create and persist a new entity for the date if it does not fall on the weekend.
            if ((startDate.get(Calendar.DAY_OF_WEEK) != Calendar.SATURDAY) && (startDate.get(Calendar.DAY_OF_WEEK) != Calendar.SUNDAY))
            {
                expectedPartitionValues.add(new SimpleDateFormat(AbstractHerdDao.DEFAULT_SINGLE_DAY_DATE_MASK).format(startDate.getTime()));
            }

            // Add one day to the calendar.
            startDate.add(Calendar.DAY_OF_MONTH, 1);
        }

        // Return the list of entities.
        return createExpectedPartitionValueEntities(partitionKeyGroupName, expectedPartitionValues);
    }

    /**
     * Creates a list of expected partition value process dates for the month of April, 2014, excluding weekends.
     *
     * @param partitionKeyGroupName the partition key group name
     *
     * @return the list of expected partition value process dates
     */
    public List<ExpectedPartitionValueEntity> createExpectedPartitionValueProcessDatesForApril2014(String partitionKeyGroupName)
    {
        return createExpectedPartitionValueProcessDates(partitionKeyGroupName, new GregorianCalendar(2014, 3, 1), new GregorianCalendar(2014, 3, 30));
    }

    /**
     * Returns an unsorted list of test expected partition values.
     *
     * @return the unsorted list of expected partition values
     */
    public List<String> getTestUnsortedExpectedPartitionValues()
    {
        return Arrays.asList("2014-04-02", "2014-04-04", "2014-04-03", "2014-04-08", "2014-04-07", "2014-04-05", "2014-04-06");
    }

    /**
     * Returns a sorted list of test expected partition values.
     *
     * @return the list of expected partition values in ascending order
     */
    public List<String> getTestSortedExpectedPartitionValues()
    {
        List<String> expectedPartitionValues = getTestUnsortedExpectedPartitionValues();
        Collections.sort(expectedPartitionValues);
        return expectedPartitionValues;
    }
}
