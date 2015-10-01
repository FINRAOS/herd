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
package org.finra.dm.service.activiti.task;

import org.activiti.engine.delegate.BpmnError;
import org.activiti.engine.delegate.DelegateExecution;
import org.activiti.engine.delegate.Expression;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.dm.service.EmrService;

@Component
public class MockJavaDelegate extends BaseJavaDelegate
{
    public static final String EXCEPTION_BPMN_ERROR = BpmnError.class.getCanonicalName();
    public static final String EXCEPTION_RUNTIME = RuntimeException.class.getCanonicalName();

    private Expression exceptionToThrow;

    @Autowired
    private EmrService emrService;

    @Override
    @SuppressWarnings("all")
    public void executeImpl(DelegateExecution execution) throws Exception
    {
        if (activitiHelper.getExpressionVariableAsString(exceptionToThrow, execution) != null)
        {
            if (activitiHelper.getExpressionVariableAsString(exceptionToThrow, execution).equals(EXCEPTION_BPMN_ERROR))
            {
                throw new BpmnError(EXCEPTION_BPMN_ERROR);
            }
            else if (activitiHelper.getExpressionVariableAsString(exceptionToThrow, execution).equals(EXCEPTION_RUNTIME))
            {
                throw new RuntimeException(EXCEPTION_RUNTIME);
            }
        }
    }
}
