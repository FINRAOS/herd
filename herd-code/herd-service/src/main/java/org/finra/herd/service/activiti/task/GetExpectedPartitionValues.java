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

import org.finra.herd.model.api.xml.ExpectedPartitionValuesInformation;
import org.finra.herd.model.api.xml.PartitionKeyGroupKey;
import org.finra.herd.model.api.xml.PartitionValueRange;
import org.finra.herd.service.ExpectedPartitionValueService;

/**
 * An Activiti task that gets a range of existing expected partition values
 * <p/>
 * <p/>
 * <pre>
 * <extensionElements>
 *   <activiti:field name="partitionKeyGroupName" stringValue="" />
 *   <activiti:field name="startExpectedPartitionValue" stringValue="" />
 *   <activiti:field name="endExpectedPartitionValue" stringValue="" />
 * </extensionElements>
 * </pre>
 */
@Component
public class GetExpectedPartitionValues extends BaseJavaDelegate
{
    private Expression partitionKeyGroupName;

    private Expression startExpectedPartitionValue;

    private Expression endExpectedPartitionValue;

    @Autowired
    private ExpectedPartitionValueService expectedPartitionValueService;

    @Override
    public void executeImpl(DelegateExecution execution) throws Exception
    {
        String partitionKeyGroupName = activitiHelper.getExpressionVariableAsString(this.partitionKeyGroupName, execution);
        String startExpectedPartitionValue = activitiHelper.getExpressionVariableAsString(this.startExpectedPartitionValue, execution);
        String endExpectedPartitionValue = activitiHelper.getExpressionVariableAsString(this.endExpectedPartitionValue, execution);

        // Get the expected partition values based on the partition key group name and the start and end expected value partition.
        ExpectedPartitionValuesInformation expectedPartitionValuesInformation =
            expectedPartitionValueService.getExpectedPartitionValues(new PartitionKeyGroupKey(partitionKeyGroupName),
                new PartitionValueRange(startExpectedPartitionValue, endExpectedPartitionValue));

        // Set JSON response workflow variable based on the result expected partition values information.
        setJsonResponseAsWorkflowVariable(expectedPartitionValuesInformation, execution);
    }
}
