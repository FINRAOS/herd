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

import org.finra.herd.model.api.xml.ExpectedPartitionValuesInformation;
import org.finra.herd.model.api.xml.Parameter;
import org.finra.herd.model.api.xml.PartitionKeyGroupKey;
import org.finra.herd.model.api.xml.PartitionValueRange;
import org.finra.herd.model.jpa.PartitionKeyGroupEntity;
import org.finra.herd.service.activiti.ActivitiRuntimeHelper;

/**
 * Test suite for the Get Expected Partition Value Activiti wrapper.
 */
public class GetExpectedPartitionValuesTest extends HerdActivitiServiceTaskTest
{
    private static final String TEST_START_EXPECTED_PARTITION_VALUE = "2014-04-02";

    private static final String TEST_END_EXPECTED_PARTITION_VALUE = "2014-04-08";

    private static final String VARIABLE_PARTITION_KEY_GROUP_NAME = "partitionKeyGroupName";

    private static final String VARIABLE_START_EXPECTED_PARTITION_VALUE = "startExpectedPartitionValue";

    private static final String VARIABLE_END_EXPECTED_PARTITION_VALUE = "endExpectedPartitionValue";

    /**
     * This unit test passes all required and optional parameters.
     */
    @Test
    public void testGetExpectedPartitionValues() throws Exception
    {
        // Create and persist a partition key group entity.
        PartitionKeyGroupEntity partitionKeyGroupEntity = partitionKeyGroupDaoTestHelper.createPartitionKeyGroupEntity(PARTITION_KEY_GROUP);

        // Create and persist a list of expected partition values.
        expectedPartitionValueDaoTestHelper
            .createExpectedPartitionValueEntities(partitionKeyGroupEntity, expectedPartitionValueDaoTestHelper.getTestUnsortedExpectedPartitionValues());

        // Get a sorted list of expected partition values.
        List<String> testSortedExpectedPartitionValues = expectedPartitionValueDaoTestHelper.getTestSortedExpectedPartitionValues();

        List<FieldExtension> fieldExtensionList = new ArrayList<>();
        fieldExtensionList.add(buildFieldExtension(VARIABLE_PARTITION_KEY_GROUP_NAME, "${partitionKeyGroupName}"));
        fieldExtensionList.add(buildFieldExtension(VARIABLE_START_EXPECTED_PARTITION_VALUE, "${startExpectedPartitionValue}"));
        fieldExtensionList.add(buildFieldExtension(VARIABLE_END_EXPECTED_PARTITION_VALUE, "${endExpectedPartitionValue}"));

        List<Parameter> parameters = new ArrayList<>();
        parameters.add(buildParameter(VARIABLE_PARTITION_KEY_GROUP_NAME, PARTITION_KEY_GROUP));
        parameters.add(buildParameter(VARIABLE_START_EXPECTED_PARTITION_VALUE, TEST_START_EXPECTED_PARTITION_VALUE));
        parameters.add(buildParameter(VARIABLE_END_EXPECTED_PARTITION_VALUE, TEST_END_EXPECTED_PARTITION_VALUE));

        // Get the expected partition values based on the partition key group name and the start and end expected value partition.
        ExpectedPartitionValuesInformation expectedPartitionValuesInformation =
            expectedPartitionValueService.getExpectedPartitionValues(new PartitionKeyGroupKey(PARTITION_KEY_GROUP),
                new PartitionValueRange(TEST_START_EXPECTED_PARTITION_VALUE, TEST_END_EXPECTED_PARTITION_VALUE));

        // Retrieve and validate the expected partition value information.
        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(GetExpectedPartitionValues.VARIABLE_JSON_RESPONSE, jsonHelper.objectToJson(expectedPartitionValuesInformation));
        testActivitiServiceTaskSuccess(GetExpectedPartitionValues.class.getCanonicalName(), fieldExtensionList, parameters, variableValuesToValidate);
    }

    /**
     * This unit test passes all required and one optional parameter.
     */
    @Test
    public void testGetExpectedPartitionValuesStartExpected() throws Exception
    {
        // Create and persist a partition key group entity.
        PartitionKeyGroupEntity partitionKeyGroupEntity = partitionKeyGroupDaoTestHelper.createPartitionKeyGroupEntity(PARTITION_KEY_GROUP);

        // Create and persist a list of expected partition values.
        expectedPartitionValueDaoTestHelper
            .createExpectedPartitionValueEntities(partitionKeyGroupEntity, expectedPartitionValueDaoTestHelper.getTestUnsortedExpectedPartitionValues());

        // Get a sorted list of expected partition values.
        List<String> testSortedExpectedPartitionValues = expectedPartitionValueDaoTestHelper.getTestSortedExpectedPartitionValues();

        List<FieldExtension> fieldExtensionList = new ArrayList<>();
        fieldExtensionList.add(buildFieldExtension(VARIABLE_PARTITION_KEY_GROUP_NAME, "${partitionKeyGroupName}"));
        fieldExtensionList.add(buildFieldExtension(VARIABLE_START_EXPECTED_PARTITION_VALUE, "${startExpectedPartitionValue}"));

        List<Parameter> parameters = new ArrayList<>();
        parameters.add(buildParameter(VARIABLE_PARTITION_KEY_GROUP_NAME, PARTITION_KEY_GROUP));
        parameters.add(buildParameter(VARIABLE_START_EXPECTED_PARTITION_VALUE, TEST_START_EXPECTED_PARTITION_VALUE));

        // Get the expected partition values based on the partition key group name and the start and end expected value partition.
        ExpectedPartitionValuesInformation expectedPartitionValuesInformation =
            expectedPartitionValueService.getExpectedPartitionValues(new PartitionKeyGroupKey(PARTITION_KEY_GROUP),
                new PartitionValueRange(TEST_START_EXPECTED_PARTITION_VALUE, null));

        // Retrieve and validate the expected partition value information.
        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(GetExpectedPartitionValues.VARIABLE_JSON_RESPONSE, jsonHelper.objectToJson(expectedPartitionValuesInformation));
        testActivitiServiceTaskSuccess(GetExpectedPartitionValues.class.getCanonicalName(), fieldExtensionList, parameters, variableValuesToValidate);
    }

    /**
     * This unit test passes all required and one optional parameter.
     */
    @Test
    public void testGetExpectedPartitionValuesEndExpected() throws Exception
    {
        // Create and persist a partition key group entity.
        PartitionKeyGroupEntity partitionKeyGroupEntity = partitionKeyGroupDaoTestHelper.createPartitionKeyGroupEntity(PARTITION_KEY_GROUP);

        // Create and persist a list of expected partition values.
        expectedPartitionValueDaoTestHelper
            .createExpectedPartitionValueEntities(partitionKeyGroupEntity, expectedPartitionValueDaoTestHelper.getTestUnsortedExpectedPartitionValues());

        // Get a sorted list of expected partition values.
        List<String> testSortedExpectedPartitionValues = expectedPartitionValueDaoTestHelper.getTestSortedExpectedPartitionValues();

        List<FieldExtension> fieldExtensionList = new ArrayList<>();
        fieldExtensionList.add(buildFieldExtension(VARIABLE_PARTITION_KEY_GROUP_NAME, "${partitionKeyGroupName}"));
        fieldExtensionList.add(buildFieldExtension(VARIABLE_END_EXPECTED_PARTITION_VALUE, "${endExpectedPartitionValue}"));

        List<Parameter> parameters = new ArrayList<>();
        parameters.add(buildParameter(VARIABLE_PARTITION_KEY_GROUP_NAME, PARTITION_KEY_GROUP));
        parameters.add(buildParameter(VARIABLE_END_EXPECTED_PARTITION_VALUE, TEST_END_EXPECTED_PARTITION_VALUE));

        // Get the expected partition values based on the partition key group name and the start and end expected value partition.
        ExpectedPartitionValuesInformation expectedPartitionValuesInformation =
            expectedPartitionValueService.getExpectedPartitionValues(new PartitionKeyGroupKey(PARTITION_KEY_GROUP),
                new PartitionValueRange(null, TEST_END_EXPECTED_PARTITION_VALUE));

        // Retrieve and validate the expected partition value information.
        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(GetExpectedPartitionValues.VARIABLE_JSON_RESPONSE, jsonHelper.objectToJson(expectedPartitionValuesInformation));
        testActivitiServiceTaskSuccess(GetExpectedPartitionValues.class.getCanonicalName(), fieldExtensionList, parameters, variableValuesToValidate);
    }

    /**
     * This unit test covers scenario when expected partition values service fails due to a missing required parameter.
     */
    @Test
    public void testGetExpectedPartitionValuesMissingPartitionKeyGroupName() throws Exception
    {
        List<FieldExtension> fieldExtensionList = new ArrayList<>();

        List<Parameter> parameters = new ArrayList<>();

        executeWithoutLogging(ActivitiRuntimeHelper.class, () -> {
            // Try to get an expected partition values information when partition key group name is not specified.
            Map<String, Object> variableValuesToValidate = new HashMap<>();
            variableValuesToValidate.put(ActivitiRuntimeHelper.VARIABLE_ERROR_MESSAGE, "A partition key group name must be specified.");
            testActivitiServiceTaskFailure(GetExpectedPartitionValues.class.getCanonicalName(), fieldExtensionList, parameters, variableValuesToValidate);
        });
    }

    /**
     * This unit test covers scenario when expected partition values service fails due to a missing required parameter.
     */
    @Test
    public void testGetExpectedPartitionValuesMissingPartitionRange() throws Exception
    {
        List<FieldExtension> fieldExtensionList = new ArrayList<>();
        fieldExtensionList.add(buildFieldExtension(VARIABLE_PARTITION_KEY_GROUP_NAME, "${partitionKeyGroupName}"));

        List<Parameter> parameters = new ArrayList<>();
        parameters.add(buildParameter(VARIABLE_PARTITION_KEY_GROUP_NAME, PARTITION_KEY_GROUP));

        executeWithoutLogging(ActivitiRuntimeHelper.class, () -> {
            // Try to get an expected partition values information when partition key group name is not specified.
            Map<String, Object> variableValuesToValidate = new HashMap<>();
            variableValuesToValidate.put(ActivitiRuntimeHelper.VARIABLE_ERROR_MESSAGE, "At least one start or end expected partition value must be specified.");
            testActivitiServiceTaskFailure(GetExpectedPartitionValues.class.getCanonicalName(), fieldExtensionList, parameters, variableValuesToValidate);
        });
    }

    /**
     * This unit test covers scenario when expected partition values service fails due to a non-existing partition key group name.
     */
    @Test
    public void testGetExpectedPartitionValuesPartitionKeyGroupNoExists() throws Exception
    {
        List<FieldExtension> fieldExtensionList = new ArrayList<>();
        fieldExtensionList.add(buildFieldExtension(VARIABLE_PARTITION_KEY_GROUP_NAME, "${partitionKeyGroupName}"));
        fieldExtensionList.add(buildFieldExtension(VARIABLE_START_EXPECTED_PARTITION_VALUE, "${startExpectedPartitionValue}"));
        fieldExtensionList.add(buildFieldExtension(VARIABLE_END_EXPECTED_PARTITION_VALUE, "${endExpectedPartitionValue}"));

        List<Parameter> parameters = new ArrayList<>();
        parameters.add(buildParameter(VARIABLE_PARTITION_KEY_GROUP_NAME, PARTITION_KEY_GROUP));
        parameters.add(buildParameter(VARIABLE_START_EXPECTED_PARTITION_VALUE, TEST_START_EXPECTED_PARTITION_VALUE));
        parameters.add(buildParameter(VARIABLE_END_EXPECTED_PARTITION_VALUE, TEST_END_EXPECTED_PARTITION_VALUE));

        executeWithoutLogging(ActivitiRuntimeHelper.class, () -> {
            // Try to get an expected partition value for a non-existing partition key group.
            Map<String, Object> variableValuesToValidate = new HashMap<>();
            variableValuesToValidate
                .put(ActivitiRuntimeHelper.VARIABLE_ERROR_MESSAGE, String.format("Partition key group \"%s\" doesn't exist.", PARTITION_KEY_GROUP));
            testActivitiServiceTaskFailure(GetExpectedPartitionValues.class.getCanonicalName(), fieldExtensionList, parameters, variableValuesToValidate);
        });
    }

    /**
     * This unit test covers scenario when expected partition values service returns an empty list of expected partition values.
     */
    @Test
    public void testGetExpectedPartitionValuesExpectedPartitionValuesNoExists() throws Exception
    {
        // Create and persist a partition key group entity.
        partitionKeyGroupDaoTestHelper.createPartitionKeyGroupEntity(PARTITION_KEY_GROUP);

        List<FieldExtension> fieldExtensionList = new ArrayList<>();
        fieldExtensionList.add(buildFieldExtension(VARIABLE_PARTITION_KEY_GROUP_NAME, "${partitionKeyGroupName}"));
        fieldExtensionList.add(buildFieldExtension(VARIABLE_START_EXPECTED_PARTITION_VALUE, "${startExpectedPartitionValue}"));
        fieldExtensionList.add(buildFieldExtension(VARIABLE_END_EXPECTED_PARTITION_VALUE, "${endExpectedPartitionValue}"));

        List<Parameter> parameters = new ArrayList<>();
        parameters.add(buildParameter(VARIABLE_PARTITION_KEY_GROUP_NAME, PARTITION_KEY_GROUP));
        parameters.add(buildParameter(VARIABLE_START_EXPECTED_PARTITION_VALUE, TEST_START_EXPECTED_PARTITION_VALUE));
        parameters.add(buildParameter(VARIABLE_END_EXPECTED_PARTITION_VALUE, TEST_END_EXPECTED_PARTITION_VALUE));

        // Get the expected partition values based on the partition key group name and the start and end expected value partition.
        ExpectedPartitionValuesInformation expectedPartitionValuesInformation =
            expectedPartitionValueService.getExpectedPartitionValues(new PartitionKeyGroupKey(PARTITION_KEY_GROUP),
                new PartitionValueRange(TEST_START_EXPECTED_PARTITION_VALUE, TEST_END_EXPECTED_PARTITION_VALUE));

        // Retrieve and validate the expected partition value information.
        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(GetExpectedPartitionValues.VARIABLE_JSON_RESPONSE, jsonHelper.objectToJson(expectedPartitionValuesInformation));
        testActivitiServiceTaskSuccess(GetExpectedPartitionValues.class.getCanonicalName(), fieldExtensionList, parameters, variableValuesToValidate);
    }
}
