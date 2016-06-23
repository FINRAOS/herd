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

import java.util.Map;

import org.activiti.engine.delegate.DelegateExecution;
import org.activiti.engine.delegate.Expression;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * An Activiti task that logs all the process variables. Although this class or member variables may look unused within an IDE, it will be dynamically called by
 * Activiti when placed in a workflow. Expressions will be injected by Activiti.
 */
@Component
public class LogVariables extends BaseJavaDelegate
{
    private static final Logger LOGGER = LoggerFactory.getLogger(LogVariables.class);

    /**
     * A regular expression that determines whether a process variable will be logged. This should be defined in the workflow as a field. For example:
     * <p/>
     * <pre>
     * <extensionElements>
     *   <activiti:field name="regex" stringValue=".*key.*" />
     * </extensionElements>
     * </pre>
     */
    private Expression regex;

    @Override
    public void executeImpl(DelegateExecution execution) throws Exception
    {
        // Get the REGEX parameter value.
        String regexValue = activitiHelper.getExpressionVariableAsString(regex, execution);

        // Loop through all process variables.
        Map<String, Object> variableMap = execution.getVariables();
        for (Map.Entry<String, Object> variableEntry : variableMap.entrySet())
        {
            // If a REGEX wasn't specified or if the variable key matches the REGEX, log the variable. Otherwise, skip it.
            if (StringUtils.isBlank(regexValue) || variableEntry.getKey().matches(regexValue))
            {
                LOGGER.info("{} Process Variable {}=\"{}\"", activitiHelper.getProcessIdentifyingInformation(execution), variableEntry.getKey(),
                    variableEntry.getValue());
            }
        }
    }
}
