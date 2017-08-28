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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.activiti.engine.delegate.DelegateExecution;
import org.activiti.engine.delegate.Expression;
import org.junit.Test;

import org.finra.herd.service.AbstractServiceTest;

/**
 * Tests the ActivitiHelper class.
 */
public class ActivitiHelperTest extends AbstractServiceTest
{
    @Test
    public void testGetExpressionVariableAsBoolean()
    {
        // Mock dependencies.
        Expression expression = mock(Expression.class);
        DelegateExecution execution = mock(DelegateExecution.class);
        when(expression.getValue(execution)).thenReturn(BOOLEAN_VALUE.toString());

        // Call the method under test.
        Boolean result = activitiHelper.getExpressionVariableAsBoolean(expression, execution, VARIABLE_NAME, NO_VARIABLE_REQUIRED, NO_BOOLEAN_DEFAULT_VALUE);

        // Validate the result.
        assertEquals(BOOLEAN_VALUE, result);
    }

    @Test
    public void testGetExpressionVariableAsBooleanBlankValue()
    {
        // Mock dependencies.
        Expression expression = mock(Expression.class);
        DelegateExecution execution = mock(DelegateExecution.class);
        when(expression.getValue(execution)).thenReturn(BLANK_TEXT);

        // Call the method under test.
        Boolean result = activitiHelper.getExpressionVariableAsBoolean(expression, execution, VARIABLE_NAME, NO_VARIABLE_REQUIRED, BOOLEAN_DEFAULT_VALUE);

        // Validate the result.
        assertEquals(BOOLEAN_DEFAULT_VALUE, result);
    }

    @Test
    public void testGetExpressionVariableAsBooleanInvalidValue()
    {
        // Mock dependencies.
        Expression expression = mock(Expression.class);
        DelegateExecution execution = mock(DelegateExecution.class);
        when(expression.getValue(execution)).thenReturn(INVALID_VALUE);

        // Try to call the method under test.
        try
        {
            activitiHelper.getExpressionVariableAsBoolean(expression, execution, VARIABLE_NAME, NO_VARIABLE_REQUIRED, NO_BOOLEAN_DEFAULT_VALUE);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("\"%s\" must be a valid boolean value of \"true\" or \"false\".", VARIABLE_NAME), e.getMessage());
        }
    }

    @Test
    public void testGetExpressionVariableAsBooleanRequired()
    {
        // Mock dependencies.
        Expression expression = mock(Expression.class);
        DelegateExecution execution = mock(DelegateExecution.class);
        when(expression.getValue(execution)).thenReturn(BOOLEAN_VALUE.toString());

        // Call the method under test.
        Boolean result = activitiHelper.getExpressionVariableAsBoolean(expression, execution, VARIABLE_NAME, VARIABLE_REQUIRED, NO_BOOLEAN_DEFAULT_VALUE);

        // Validate the result.
        assertEquals(BOOLEAN_VALUE, result);
    }

    @Test
    public void testGetExpressionVariableAsBooleanRequiredBlankValue()
    {
        // Mock dependencies.
        Expression expression = mock(Expression.class);
        DelegateExecution execution = mock(DelegateExecution.class);
        when(expression.getValue(execution)).thenReturn(BLANK_TEXT);

        // Try to call the method under test.
        try
        {
            activitiHelper.getExpressionVariableAsBoolean(expression, execution, VARIABLE_NAME, VARIABLE_REQUIRED, NO_BOOLEAN_DEFAULT_VALUE);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("\"%s\" must be specified.", VARIABLE_NAME), e.getMessage());
        }
    }

    @Test
    public void testGetExpressionVariableAsInteger()
    {
        // Mock dependencies.
        Expression expression = mock(Expression.class);
        DelegateExecution execution = mock(DelegateExecution.class);
        when(expression.getValue(execution)).thenReturn(INTEGER_VALUE.toString());

        // Call the method under test.
        Integer result = activitiHelper.getExpressionVariableAsInteger(expression, execution, VARIABLE_NAME, NO_VARIABLE_REQUIRED);

        // Validate the result.
        assertEquals(INTEGER_VALUE, result);
    }

    @Test
    public void testGetExpressionVariableAsIntegerBlankValue()
    {
        // Mock dependencies.
        Expression expression = mock(Expression.class);
        DelegateExecution execution = mock(DelegateExecution.class);
        when(expression.getValue(execution)).thenReturn(BLANK_TEXT);

        // Call the method under test.
        Integer result = activitiHelper.getExpressionVariableAsInteger(expression, execution, VARIABLE_NAME, NO_VARIABLE_REQUIRED);

        // Validate the result.
        assertNull(result);
    }

    @Test
    public void testGetExpressionVariableAsIntegerInvalidValue()
    {
        // Mock dependencies.
        Expression expression = mock(Expression.class);
        DelegateExecution execution = mock(DelegateExecution.class);
        when(expression.getValue(execution)).thenReturn(INVALID_VALUE);

        // Try to call the method under test.
        try
        {
            activitiHelper.getExpressionVariableAsInteger(expression, execution, VARIABLE_NAME, VARIABLE_REQUIRED);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("\"%s\" must be a valid integer value.", VARIABLE_NAME), e.getMessage());
        }
    }

    @Test
    public void testGetExpressionVariableAsIntegerRequired()
    {
        // Mock dependencies.
        Expression expression = mock(Expression.class);
        DelegateExecution execution = mock(DelegateExecution.class);
        when(expression.getValue(execution)).thenReturn(INTEGER_VALUE.toString());

        // Call the method under test.
        Integer result = activitiHelper.getExpressionVariableAsInteger(expression, execution, VARIABLE_NAME, VARIABLE_REQUIRED);

        // Validate the result.
        assertEquals(INTEGER_VALUE, result);
    }

    @Test
    public void testGetExpressionVariableAsIntegerRequiredBlankValue()
    {
        // Mock dependencies.
        Expression expression = mock(Expression.class);
        DelegateExecution execution = mock(DelegateExecution.class);
        when(expression.getValue(execution)).thenReturn(BLANK_TEXT);

        // Try to call the method under test.
        try
        {
            activitiHelper.getExpressionVariableAsInteger(expression, execution, VARIABLE_NAME, VARIABLE_REQUIRED);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("\"%s\" must be specified.", VARIABLE_NAME), e.getMessage());
        }
    }

    @Test
    public void testGetProcessIdentifyingInformation()
    {
        // Create variables required for testing.
        final String processDefinitionId = STRING_VALUE;
        final String processInstanceId = STRING_VALUE_2;

        // Mock dependencies.
        DelegateExecution execution = mock(DelegateExecution.class);
        when(execution.getProcessDefinitionId()).thenReturn(processDefinitionId);
        when(execution.getProcessInstanceId()).thenReturn(processInstanceId);

        // Call the method under test.
        String result = activitiHelper.getProcessIdentifyingInformation(execution);

        // Validate the result.
        assertEquals(String.format("[ activitiProcessDefinitionId=\"%s\" activitiProcessInstanceId=\"%s\" ]", processDefinitionId, processInstanceId), result);
    }

    @Test
    public void testGetRequiredExpressionVariableAsString()
    {
        // Mock dependencies.
        Expression expression = mock(Expression.class);
        DelegateExecution execution = mock(DelegateExecution.class);
        when(expression.getValue(execution)).thenReturn(STRING_VALUE);

        // Call the method under test.
        String result = activitiHelper.getRequiredExpressionVariableAsString(expression, execution, VARIABLE_NAME);

        // Validate the result.
        assertEquals(STRING_VALUE, result);
    }

    @Test
    public void testGetRequiredExpressionVariableAsStringBlankValue()
    {
        // Mock dependencies.
        Expression expression = mock(Expression.class);
        DelegateExecution execution = mock(DelegateExecution.class);
        when(expression.getValue(execution)).thenReturn(BLANK_TEXT);

        // Try to call the method under test.
        try
        {
            activitiHelper.getRequiredExpressionVariableAsString(expression, execution, VARIABLE_NAME);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("\"%s\" must be specified.", VARIABLE_NAME), e.getMessage());
        }
    }
}
