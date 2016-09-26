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
import static org.junit.Assert.assertNotNull;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.model.api.xml.ExpectedPartitionValueInformation;
import org.finra.herd.model.api.xml.ExpectedPartitionValuesCreateRequest;
import org.finra.herd.model.api.xml.ExpectedPartitionValuesDeleteRequest;
import org.finra.herd.model.api.xml.ExpectedPartitionValuesInformation;

@Component
public class ExpectedPartitionValueServiceTestHelper
{
    @Autowired
    private PartitionKeyGroupServiceTestHelper partitionKeyGroupServiceTestHelper;

    /**
     * Creates an expected partition values create request.
     *
     * @param partitionKeyGroupName the partition key group name
     * @param expectedPartitionValues the list of expected partition values
     *
     * @return the expected partition values create request
     */
    public ExpectedPartitionValuesCreateRequest createExpectedPartitionValuesCreateRequest(String partitionKeyGroupName, List<String> expectedPartitionValues)
    {
        ExpectedPartitionValuesCreateRequest expectedPartitionValuesCreateRequest = new ExpectedPartitionValuesCreateRequest();
        expectedPartitionValuesCreateRequest.setPartitionKeyGroupKey(partitionKeyGroupServiceTestHelper.createPartitionKeyGroupKey(partitionKeyGroupName));
        expectedPartitionValuesCreateRequest.setExpectedPartitionValues(expectedPartitionValues);
        return expectedPartitionValuesCreateRequest;
    }

    /**
     * Creates an expected partition values delete request.
     *
     * @param partitionKeyGroupName the partition key group name
     * @param expectedPartitionValues the list of expected partition values
     *
     * @return the expected partition values delete request
     */
    public ExpectedPartitionValuesDeleteRequest createExpectedPartitionValuesDeleteRequest(String partitionKeyGroupName, List<String> expectedPartitionValues)
    {
        ExpectedPartitionValuesDeleteRequest expectedPartitionValuesDeleteRequest = new ExpectedPartitionValuesDeleteRequest();
        expectedPartitionValuesDeleteRequest.setPartitionKeyGroupKey(partitionKeyGroupServiceTestHelper.createPartitionKeyGroupKey(partitionKeyGroupName));
        expectedPartitionValuesDeleteRequest.setExpectedPartitionValues(expectedPartitionValues);
        return expectedPartitionValuesDeleteRequest;
    }

    /**
     * Validates expected partition value information contents against specified arguments.
     *
     * @param expectedPartitionKeyGroupName the expected partition key group name
     * @param expectedExpectedPartitionValue the expected value of the expected partition value
     * @param actualExpectedPartitionValueInformation the expected partition value information to be validated
     */
    public void validateExpectedPartitionValueInformation(String expectedPartitionKeyGroupName, String expectedExpectedPartitionValue,
        ExpectedPartitionValueInformation actualExpectedPartitionValueInformation)
    {
        assertNotNull(actualExpectedPartitionValueInformation);
        assertEquals(expectedPartitionKeyGroupName, actualExpectedPartitionValueInformation.getExpectedPartitionValueKey().getPartitionKeyGroupName());
        assertEquals(expectedExpectedPartitionValue, actualExpectedPartitionValueInformation.getExpectedPartitionValueKey().getExpectedPartitionValue());
    }

    /**
     * Validates expected partition values information contents against specified arguments.
     *
     * @param expectedPartitionKeyGroupName the expected partition key group name
     * @param expectedExpectedPartitionValues the expected list of expected partition values
     * @param actualExpectedPartitionValuesInformation the expected partition values information to be validated
     */
    public void validateExpectedPartitionValuesInformation(String expectedPartitionKeyGroupName, List<String> expectedExpectedPartitionValues,
        ExpectedPartitionValuesInformation actualExpectedPartitionValuesInformation)
    {
        assertNotNull(actualExpectedPartitionValuesInformation);
        assertEquals(expectedPartitionKeyGroupName, actualExpectedPartitionValuesInformation.getPartitionKeyGroupKey().getPartitionKeyGroupName());
        assertEquals(expectedExpectedPartitionValues, actualExpectedPartitionValuesInformation.getExpectedPartitionValues());
    }
}
