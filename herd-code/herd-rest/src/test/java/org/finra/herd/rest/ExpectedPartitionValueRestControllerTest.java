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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.model.api.xml.ExpectedPartitionValueInformation;
import org.finra.herd.model.api.xml.ExpectedPartitionValueKey;
import org.finra.herd.model.api.xml.ExpectedPartitionValuesCreateRequest;
import org.finra.herd.model.api.xml.ExpectedPartitionValuesDeleteRequest;
import org.finra.herd.model.api.xml.ExpectedPartitionValuesInformation;
import org.finra.herd.model.api.xml.PartitionKeyGroupKey;
import org.finra.herd.model.api.xml.PartitionValueRange;
import org.finra.herd.service.ExpectedPartitionValueService;

/**
 * This class tests various functionality within the expected partition value REST controller.
 */
public class ExpectedPartitionValueRestControllerTest extends AbstractRestTest
{
    @InjectMocks
    private ExpectedPartitionValueRestController expectedPartitionValueRestController;

    @Mock
    private ExpectedPartitionValueService expectedPartitionValueService;

    @Before()
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCreateExpectedPartitionValues()
    {
        ExpectedPartitionValuesInformation expectedPartitionValuesInformation =
            new ExpectedPartitionValuesInformation(partitionKeyGroupServiceTestHelper.createPartitionKeyGroupKey(PARTITION_KEY_GROUP),
                expectedPartitionValueDaoTestHelper.getTestSortedExpectedPartitionValues());
        ExpectedPartitionValuesCreateRequest request = expectedPartitionValueServiceTestHelper
            .createExpectedPartitionValuesCreateRequest(PARTITION_KEY_GROUP, expectedPartitionValueDaoTestHelper.getTestUnsortedExpectedPartitionValues());

        when(expectedPartitionValueService.createExpectedPartitionValues(request)).thenReturn(expectedPartitionValuesInformation);

        ExpectedPartitionValuesInformation resultPartitionValuesInformation = expectedPartitionValueRestController.createExpectedPartitionValues(request);

        // Validate the returned object.
        expectedPartitionValueServiceTestHelper
            .validateExpectedPartitionValuesInformation(PARTITION_KEY_GROUP, expectedPartitionValueDaoTestHelper.getTestSortedExpectedPartitionValues(),
                resultPartitionValuesInformation);

        // Verify the external calls.
        verify(expectedPartitionValueService).createExpectedPartitionValues(request);
        verifyNoMoreInteractions(expectedPartitionValueService);
        // Validate the returned object.
        assertEquals(expectedPartitionValuesInformation, resultPartitionValuesInformation);
    }

    @Test
    public void testDeleteExpectedPartitionValues()
    {
        ExpectedPartitionValuesInformation expectedPartitionValuesInformation =
            new ExpectedPartitionValuesInformation(partitionKeyGroupServiceTestHelper.createPartitionKeyGroupKey(PARTITION_KEY_GROUP),
                expectedPartitionValueDaoTestHelper.getTestSortedExpectedPartitionValues());
        // Delete expected partition values from this partition key group.
        ExpectedPartitionValuesDeleteRequest request = expectedPartitionValueServiceTestHelper
            .createExpectedPartitionValuesDeleteRequest(PARTITION_KEY_GROUP, expectedPartitionValueDaoTestHelper.getTestUnsortedExpectedPartitionValues());

        when(expectedPartitionValueService.deleteExpectedPartitionValues(request)).thenReturn(expectedPartitionValuesInformation);

        ExpectedPartitionValuesInformation resultPartitionValuesInformation = expectedPartitionValueRestController.deleteExpectedPartitionValues(request);

        // Validate the returned object.
        expectedPartitionValueServiceTestHelper
            .validateExpectedPartitionValuesInformation(PARTITION_KEY_GROUP, expectedPartitionValueDaoTestHelper.getTestSortedExpectedPartitionValues(),
                resultPartitionValuesInformation);

        // Verify the external calls.
        verify(expectedPartitionValueService).deleteExpectedPartitionValues(request);
        verifyNoMoreInteractions(expectedPartitionValueService);
        // Validate the returned object.
        assertEquals(expectedPartitionValuesInformation, resultPartitionValuesInformation);
    }

    @Test
    public void testGetExpectedPartitionValue()
    {
        int offset = 1;
        String partitionOffset = expectedPartitionValueDaoTestHelper.getTestSortedExpectedPartitionValues().get(offset);
        ExpectedPartitionValueKey expectedPartitionValueKey = new ExpectedPartitionValueKey(PARTITION_KEY_GROUP, partitionOffset);
        ExpectedPartitionValueInformation expectedPartitionValuesInformation = new ExpectedPartitionValueInformation(expectedPartitionValueKey);

        when(expectedPartitionValueService.getExpectedPartitionValue(expectedPartitionValueKey, offset)).thenReturn(expectedPartitionValuesInformation);
        ExpectedPartitionValueInformation resultPartitionValueInformation =
            expectedPartitionValueRestController.getExpectedPartitionValue(PARTITION_KEY_GROUP, partitionOffset, offset);

        // Verify the external calls.
        verify(expectedPartitionValueService).getExpectedPartitionValue(expectedPartitionValueKey, offset);
        verifyNoMoreInteractions(expectedPartitionValueService);
        // Validate the returned object.
        assertEquals(expectedPartitionValuesInformation, resultPartitionValueInformation);
    }

    @Test
    public void testGetExpectedPartitionValues()
    {
        PartitionKeyGroupKey partitionKeyGroupKey = new PartitionKeyGroupKey();
        partitionKeyGroupKey.setPartitionKeyGroupName(PARTITION_KEY_GROUP);
        List<String> testSortedExpectedPartitionValues = expectedPartitionValueDaoTestHelper.getTestSortedExpectedPartitionValues();
        int startExpectedPartitionValueIndex = 1;
        int endExpectedPartitionValueIndex = testSortedExpectedPartitionValues.size() - 2;
        PartitionValueRange partitionValueRange = new PartitionValueRange();
        partitionValueRange.setStartPartitionValue(testSortedExpectedPartitionValues.get(startExpectedPartitionValueIndex));
        partitionValueRange.setEndPartitionValue(testSortedExpectedPartitionValues.get(endExpectedPartitionValueIndex));

        ExpectedPartitionValuesInformation expectedPartitionValuesInformation =
            new ExpectedPartitionValuesInformation(partitionKeyGroupServiceTestHelper.createPartitionKeyGroupKey(PARTITION_KEY_GROUP),
                expectedPartitionValueDaoTestHelper.getTestSortedExpectedPartitionValues()
                    .subList(startExpectedPartitionValueIndex, endExpectedPartitionValueIndex));

        when(expectedPartitionValueService.getExpectedPartitionValues(partitionKeyGroupKey, partitionValueRange))
            .thenReturn(expectedPartitionValuesInformation);

        ExpectedPartitionValuesInformation resultPartitionValuesInformation = expectedPartitionValueRestController
            .getExpectedPartitionValues(PARTITION_KEY_GROUP, testSortedExpectedPartitionValues.get(startExpectedPartitionValueIndex),
                testSortedExpectedPartitionValues.get(endExpectedPartitionValueIndex));

        // Validate the returned object.
        expectedPartitionValueServiceTestHelper.validateExpectedPartitionValuesInformation(PARTITION_KEY_GROUP,
            testSortedExpectedPartitionValues.subList(startExpectedPartitionValueIndex, endExpectedPartitionValueIndex), resultPartitionValuesInformation);
        // Verify the external calls.
        verify(expectedPartitionValueService).getExpectedPartitionValues(partitionKeyGroupKey, partitionValueRange);
        verifyNoMoreInteractions(expectedPartitionValueService);
        // Validate the returned object.
        assertEquals(expectedPartitionValuesInformation, resultPartitionValuesInformation);
    }
}
