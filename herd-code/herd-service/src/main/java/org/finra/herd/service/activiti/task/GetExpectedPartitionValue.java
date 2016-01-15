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
package org.finra.herd.service.activiti.task;

import org.activiti.engine.delegate.DelegateExecution;
import org.activiti.engine.delegate.Expression;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.model.api.xml.ExpectedPartitionValueInformation;
import org.finra.herd.model.api.xml.ExpectedPartitionValueKey;
import org.finra.herd.service.ExpectedPartitionValueService;

/**
 * An Activiti task that gets an existing expected partition value plus/minus an optional offset
 * <p/>
 * <p/>
 * <pre>
 * <extensionElements>
 *   <activiti:field name="partitionKeyGroupName" stringValue="" />
 *   <activiti:field name="expectedPartitionValue" stringValue="" />
 *   <activiti:field name="offset" stringValue="" />
 * </extensionElements>
 * </pre>
 */
@Component
public class GetExpectedPartitionValue extends BaseJavaDelegate
{
    protected static final String VARIABLE_PARTITION_KEY_GROUP_NAME = "partitionKeyGroupName";
    protected static final String VARIABLE_EXPECTED_PARTITION_VALUE = "expectedPartitionValue";
    protected static final String VARIABLE_OFFSET = "offset";

    private Expression partitionKeyGroupName;
    private Expression expectedPartitionValue;
    private Expression offset;

    @Autowired
    private ExpectedPartitionValueService expectedPartitionValueService;

    @Override
    public void executeImpl(DelegateExecution execution) throws Exception
    {
        String partitionKeyGroupName = activitiHelper.getExpressionVariableAsString(this.partitionKeyGroupName, execution);
        String expectedPartitionValue = activitiHelper.getExpressionVariableAsString(this.expectedPartitionValue, execution);
        Integer offset = activitiHelper.getExpressionVariableAsInteger(this.offset, execution, VARIABLE_OFFSET, false);

        ExpectedPartitionValueInformation expectedPartitionValueInformation =
            expectedPartitionValueService.getExpectedPartitionValue(new ExpectedPartitionValueKey(partitionKeyGroupName, expectedPartitionValue), offset);

        // Set workflow variables based on the result expected partition value information.
        setTaskWorkflowVariable(execution, VARIABLE_PARTITION_KEY_GROUP_NAME,
            expectedPartitionValueInformation.getExpectedPartitionValueKey().getPartitionKeyGroupName());
        setTaskWorkflowVariable(execution, VARIABLE_EXPECTED_PARTITION_VALUE,
            expectedPartitionValueInformation.getExpectedPartitionValueKey().getExpectedPartitionValue());
    }
}
