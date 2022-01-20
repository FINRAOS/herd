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
package org.finra.herd.service.activiti;

import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;

import javax.xml.stream.XMLStreamException;

import org.activiti.bpmn.converter.BpmnXMLConverter;
import org.activiti.bpmn.model.BpmnModel;
import org.activiti.engine.delegate.DelegateExecution;
import org.activiti.engine.delegate.Expression;
import org.activiti.engine.impl.util.io.InputStreamSource;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.helpers.MessageFormatter;
import org.springframework.stereotype.Component;

/**
 * A helper bean that provides useful methods for Activiti classes (e.g. interceptors, Java delegates, etc.).
 * <p/>
 * If you want to find common functionality related to Activiti Runtime, then use ActivitiRuntimeHelper.
 */
@Component
public class ActivitiHelper
{
    /**
     * Gets process identifying information from the current execution as a String.
     *
     * @param execution the execution.
     *
     * @return the process identifying information.
     */
    public String getProcessIdentifyingInformation(DelegateExecution execution)
    {
        return MessageFormatter.format("[ activitiProcessDefinitionId=\"{}\" activitiProcessInstanceId=\"{}\" ]", execution.getProcessDefinitionId(),
            execution.getProcessInstanceId()).getMessage();
    }

    /**
     * Gets an expression variable from the execution. This method will return null if the expression is null (i.e. when a workflow doesn't define the
     * expression).
     *
     * @param expression the expression variable to get.
     * @param execution the execution that contains the variable value.
     *
     * @return the variable value or null if the expression is null.
     */
    public Object getExpressionVariable(Expression expression, DelegateExecution execution)
    {
        return (expression == null ? null : expression.getValue(execution));
    }

    /**
     * Gets an expression variable from the execution as String. This method will return null if the expression is null (i.e. when a workflow doesn't define the
     * expression).
     *
     * @param expression the expression variable to get.
     * @param execution the execution that contains the variable value.
     *
     * @return the variable value or null if the expression is null.
     */
    public String getExpressionVariableAsString(Expression expression, DelegateExecution execution)
    {
        return (String) getExpressionVariable(expression, execution);
    }

    /**
     * Gets an expression variable from the execution as String. This method will throw IllegalArgumentException if the expression is null (i.e. when a workflow
     * doesn't define the expression).
     *
     * @param expression the expression variable to get.
     * @param execution the execution that contains the variable value.
     * @param variableName the name of the variable that is being checked.
     *
     * @return the variable value.
     * @throws IllegalArgumentException if variable is not specified.
     */
    public String getRequiredExpressionVariableAsString(Expression expression, DelegateExecution execution, String variableName) throws IllegalArgumentException
    {
        String variableString = getExpressionVariableAsString(expression, execution);

        if (StringUtils.isBlank(variableString))
        {
            throw new IllegalArgumentException("\"" + variableName + "\" must be specified.");
        }

        return variableString;
    }

    /**
     * Gets an expression variable from the execution as Integer. This method will throw IllegalArgumentException if it is not a valid integer value and is a
     * required variable.
     *
     * @param expression the expression variable to get.
     * @param execution the execution that contains the variable value.
     * @param variableName the variable name
     * @param isRequired whether this variable is required
     *
     * @return the integer variable value.
     * @throws IllegalArgumentException if not a valid integer value.
     */
    public Integer getExpressionVariableAsInteger(Expression expression, DelegateExecution execution, String variableName, boolean isRequired)
        throws IllegalArgumentException
    {
        Integer variableInteger = null;

        String variableString = getExpressionVariableAsString(expression, execution);

        if (isRequired && StringUtils.isBlank(variableString))
        {
            throw new IllegalArgumentException("\"" + variableName + "\" must be specified.");
        }
        if (isRequired || StringUtils.isNotBlank(variableString))
        {
            try
            {
                variableInteger = Integer.parseInt(getExpressionVariableAsString(expression, execution));
            }
            catch (Exception ex)
            {
                throw new IllegalArgumentException("\"" + variableName + "\" must be a valid integer value.", ex);
            }
        }
        return variableInteger;
    }

    /**
     * Gets an expression variable from the execution as a Boolean. This method will throw IllegalArgumentException if it is not a valid boolean value and is a
     * required variable.
     *
     * @param expression the expression variable to get.
     * @param execution the execution that contains the variable value.
     * @param variableName the variable name.
     * @param isRequired whether this variable is required.
     * @param defaultValue the default value if variable is null.
     *
     * @return the Boolean variable value.
     * @throws IllegalArgumentException if not a valid boolean value.
     */
    public Boolean getExpressionVariableAsBoolean(Expression expression, DelegateExecution execution, String variableName, boolean isRequired,
        Boolean defaultValue) throws IllegalArgumentException
    {
        Boolean variableBoolean = defaultValue;
        String variableString = getExpressionVariableAsString(expression, execution);

        if (isRequired && StringUtils.isBlank(variableString))
        {
            throw new IllegalArgumentException("\"" + variableName + "\" must be specified.");
        }

        if (isRequired || StringUtils.isNotBlank(variableString))
        {
            String variableStringTrimmed = variableString.trim();
            if (variableStringTrimmed.equalsIgnoreCase(Boolean.TRUE.toString()) || variableStringTrimmed.equalsIgnoreCase(Boolean.FALSE.toString()))
            {
                variableBoolean = Boolean.valueOf(variableStringTrimmed);
            }
            else
            {
                throw new IllegalArgumentException("\"" + variableName + "\" must be a valid boolean value of \"true\" or \"false\".");
            }
        }

        return variableBoolean;
    }

    /**
     * Constructs BPMN model from the XML string and validates
     *
     * @param xmlString the Activiti XML
     *
     * @return the BPMN Model.
     * @throws UnsupportedEncodingException if the encoding wasn't supported.
     * @throws XMLStreamException if the XML couldn't be streamed.
     */
    public BpmnModel constructBpmnModelFromXmlAndValidate(String xmlString) throws UnsupportedEncodingException, XMLStreamException
    {
        InputStreamSource source = new InputStreamSource(new ByteArrayInputStream(xmlString.trim().getBytes(StandardCharsets.UTF_8)));
        return new BpmnXMLConverter().convertToBpmnModel(source, true, true);
    }
}
