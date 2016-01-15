/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.finra.herd.service.activiti.task;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.activiti.bpmn.model.FieldExtension;
import org.junit.Test;

import org.finra.herd.model.api.xml.Parameter;
import org.finra.herd.model.jpa.PartitionKeyGroupEntity;
import org.finra.herd.service.activiti.ActivitiRuntimeHelper;

/**
 * Test suite for the Get Expected Partition Value Activiti wrapper.
 */
public class GetExpectedPartitionValueTest extends HerdActivitiServiceTaskTest
{
    private static final Integer TEST_EXPECTED_PARTITION_VALUE_INDEX = 3;
    private static final Integer TEST_OFFSET = -2;

    /**
     * This unit test passes all required and optional parameters.
     */
    @Test
    public void testGetExpectedPartitionValue() throws Exception
    {
        // Create and persist a partition key group entity.
        PartitionKeyGroupEntity partitionKeyGroupEntity = createPartitionKeyGroupEntity(PARTITION_KEY_GROUP);

        // Create and persist a list of expected partition values.
        createExpectedPartitionValueEntities(partitionKeyGroupEntity, getTestUnsortedExpectedPartitionValues());

        // Get a sorted list of expected partition values.
        List<String> testSortedExpectedPartitionValues = getTestSortedExpectedPartitionValues();

        List<FieldExtension> fieldExtensionList = new ArrayList<>();
        fieldExtensionList.add(buildFieldExtension(GetExpectedPartitionValue.VARIABLE_PARTITION_KEY_GROUP_NAME, "${partitionKeyGroupName}"));
        fieldExtensionList.add(buildFieldExtension(GetExpectedPartitionValue.VARIABLE_EXPECTED_PARTITION_VALUE, "${expectedPartitionValue}"));
        fieldExtensionList.add(buildFieldExtension(GetExpectedPartitionValue.VARIABLE_OFFSET, "${offset}"));

        List<Parameter> parameters = new ArrayList<>();
        parameters.add(buildParameter(GetExpectedPartitionValue.VARIABLE_PARTITION_KEY_GROUP_NAME, PARTITION_KEY_GROUP));
        parameters.add(buildParameter(GetExpectedPartitionValue.VARIABLE_EXPECTED_PARTITION_VALUE,
            testSortedExpectedPartitionValues.get(TEST_EXPECTED_PARTITION_VALUE_INDEX)));
        parameters.add(buildParameter(GetExpectedPartitionValue.VARIABLE_OFFSET, TEST_OFFSET.toString()));

        // Retrieve and validate the expected partition value information.
        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(GetExpectedPartitionValue.VARIABLE_PARTITION_KEY_GROUP_NAME, PARTITION_KEY_GROUP);
        variableValuesToValidate.put(GetExpectedPartitionValue.VARIABLE_EXPECTED_PARTITION_VALUE,
            testSortedExpectedPartitionValues.get(TEST_EXPECTED_PARTITION_VALUE_INDEX + TEST_OFFSET));
        testActivitiServiceTaskSuccess(GetExpectedPartitionValue.class.getCanonicalName(), fieldExtensionList, parameters, variableValuesToValidate);
    }

    /**
     * This unit test does not pass optional parameters.
     */
    @Test
    public void testGetExpectedPartitionValueMissingOptionalParameters() throws Exception
    {
        // Create and persist a partition key group entity.
        PartitionKeyGroupEntity partitionKeyGroupEntity = createPartitionKeyGroupEntity(PARTITION_KEY_GROUP);

        // Create and persist a list of expected partition values.
        createExpectedPartitionValueEntities(partitionKeyGroupEntity, getTestUnsortedExpectedPartitionValues());

        // Get a sorted list of expected partition values.
        List<String> testSortedExpectedPartitionValues = getTestSortedExpectedPartitionValues();

        List<FieldExtension> fieldExtensionList = new ArrayList<>();
        fieldExtensionList.add(buildFieldExtension(GetExpectedPartitionValue.VARIABLE_PARTITION_KEY_GROUP_NAME, "${partitionKeyGroupName}"));
        fieldExtensionList.add(buildFieldExtension(GetExpectedPartitionValue.VARIABLE_EXPECTED_PARTITION_VALUE, "${expectedPartitionValue}"));

        List<Parameter> parameters = new ArrayList<>();
        parameters.add(buildParameter(GetExpectedPartitionValue.VARIABLE_PARTITION_KEY_GROUP_NAME, PARTITION_KEY_GROUP));
        parameters.add(buildParameter(GetExpectedPartitionValue.VARIABLE_EXPECTED_PARTITION_VALUE,
            testSortedExpectedPartitionValues.get(TEST_EXPECTED_PARTITION_VALUE_INDEX)));

        // Retrieve and validate the expected partition value information.
        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(GetExpectedPartitionValue.VARIABLE_PARTITION_KEY_GROUP_NAME, PARTITION_KEY_GROUP);
        variableValuesToValidate
            .put(GetExpectedPartitionValue.VARIABLE_EXPECTED_PARTITION_VALUE, testSortedExpectedPartitionValues.get(TEST_EXPECTED_PARTITION_VALUE_INDEX));
        testActivitiServiceTaskSuccess(GetExpectedPartitionValue.class.getCanonicalName(), fieldExtensionList, parameters, variableValuesToValidate);
    }

    /**
     * This unit test covers scenario when expected partition value service fails due to a missing required parameter.
     */
    @Test
    public void testGetExpectedPartitionValueMissingPartitionKeyGroupName() throws Exception
    {
        List<FieldExtension> fieldExtensionList = new ArrayList<>();

        List<Parameter> parameters = new ArrayList<>();

        // Try to get an expected partition value information when partition key group name is not specified.
        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(ActivitiRuntimeHelper.VARIABLE_ERROR_MESSAGE, "A partition key group name must be specified.");
        testActivitiServiceTaskFailure(GetExpectedPartitionValue.class.getCanonicalName(), fieldExtensionList, parameters, variableValuesToValidate);
    }

    /**
     * This method tests an invalid integer value specified for the offset input parameter.
     */
    @Test
    public void testGetExpectedPartitionValueInvalidOffset() throws Exception
    {
        List<FieldExtension> fieldExtensionList = new ArrayList<>();
        fieldExtensionList.add(buildFieldExtension(GetExpectedPartitionValue.VARIABLE_OFFSET, "${offset}"));

        List<Parameter> parameters = new ArrayList<>();
        parameters.add(buildParameter(GetExpectedPartitionValue.VARIABLE_OFFSET, INVALID_INTEGER_VALUE));

        // Try to get an expected partition value instance when offset is not an integer.
        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(ActivitiRuntimeHelper.VARIABLE_ERROR_MESSAGE,
            String.format("\"%s\" must be a valid integer value.", GetExpectedPartitionValue.VARIABLE_OFFSET));
        testActivitiServiceTaskFailure(GetExpectedPartitionValue.class.getCanonicalName(), fieldExtensionList, parameters, variableValuesToValidate);
    }

    /**
     * This unit test covers scenario when expected partition value service fails due to a non-existing partition key group name.
     */
    @Test
    public void testGetExpectedPartitionValuePartitionKeyGroupNoExists() throws Exception
    {
        List<FieldExtension> fieldExtensionList = new ArrayList<>();
        fieldExtensionList.add(buildFieldExtension(GetExpectedPartitionValue.VARIABLE_PARTITION_KEY_GROUP_NAME, "${partitionKeyGroupName}"));
        fieldExtensionList.add(buildFieldExtension(GetExpectedPartitionValue.VARIABLE_EXPECTED_PARTITION_VALUE, "${expectedPartitionValue}"));

        List<Parameter> parameters = new ArrayList<>();
        parameters.add(buildParameter(GetExpectedPartitionValue.VARIABLE_PARTITION_KEY_GROUP_NAME, PARTITION_KEY_GROUP));
        parameters.add(buildParameter(GetExpectedPartitionValue.VARIABLE_EXPECTED_PARTITION_VALUE, STRING_VALUE));

        // Try to get an expected partition value for a non-existing partition key group.
        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate
            .put(ActivitiRuntimeHelper.VARIABLE_ERROR_MESSAGE, String.format("Partition key group \"%s\" doesn't exist.", PARTITION_KEY_GROUP));
        testActivitiServiceTaskFailure(GetExpectedPartitionValue.class.getCanonicalName(), fieldExtensionList, parameters, variableValuesToValidate);
    }

    /**
     * This unit test covers scenario when expected partition value service fails due to a non-existing expected partition value.
     */
    @Test
    public void testGetExpectedPartitionValueExpectedPartitionValueNoExists() throws Exception
    {
        // Create and persist a partition key group entity.
        createPartitionKeyGroupEntity(PARTITION_KEY_GROUP);

        List<FieldExtension> fieldExtensionList = new ArrayList<>();
        fieldExtensionList.add(buildFieldExtension(GetExpectedPartitionValue.VARIABLE_PARTITION_KEY_GROUP_NAME, "${partitionKeyGroupName}"));
        fieldExtensionList.add(buildFieldExtension(GetExpectedPartitionValue.VARIABLE_EXPECTED_PARTITION_VALUE, "${expectedPartitionValue}"));

        List<Parameter> parameters = new ArrayList<>();
        parameters.add(buildParameter(GetExpectedPartitionValue.VARIABLE_PARTITION_KEY_GROUP_NAME, PARTITION_KEY_GROUP));
        parameters.add(buildParameter(GetExpectedPartitionValue.VARIABLE_EXPECTED_PARTITION_VALUE, STRING_VALUE));

        // Try to get a non-existing expected partition value.
        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(ActivitiRuntimeHelper.VARIABLE_ERROR_MESSAGE,
            String.format("Expected partition value \"%s\" doesn't exist in \"%s\" partition key group.", STRING_VALUE, PARTITION_KEY_GROUP));
        testActivitiServiceTaskFailure(GetExpectedPartitionValue.class.getCanonicalName(), fieldExtensionList, parameters, variableValuesToValidate);
    }
}
