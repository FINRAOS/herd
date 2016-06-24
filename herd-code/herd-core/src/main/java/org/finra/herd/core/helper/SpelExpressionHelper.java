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
package org.finra.herd.core.helper;

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.expression.Expression;
import org.springframework.expression.ParseException;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.stereotype.Component;

/**
 * Helper that contains operations around SpEL expression parse and evaluation.
 */
@Component
public class SpelExpressionHelper
{
    @Autowired
    private SpelExpressionParser spelExpressionParser;

    /**
     * Evaluates the given expression string using the given variables as context variables.
     * 
     * @param <T> The desired type
     * @param expressionString Expression string to evaluate
     * @param desiredResultType The desired type of the result. Spring will make best attempt at converting result to this type.
     * @param variables The variables to put into context
     * @return The result of the evaluation
     */
    public <T> T evaluate(String expressionString, Class<T> desiredResultType, Map<String, Object> variables)
    {
        return evaluate(parseExpression(expressionString), desiredResultType, variables);
    }

    /**
     * Evaluates the given expression string using the given variables as context variables.
     * 
     * @param <T> The desired type
     * @param expressionString Expression string to evaluate
     * @param variables The variables to put into context
     * @return The result of the evaluation
     */
    public <T> T evaluate(String expressionString, Map<String, Object> variables)
    {
        return evaluate(parseExpression(expressionString), variables);
    }

    /**
     * Parses the given expression string into a SpEL expression.
     * 
     * @param expressionString The expression string
     * @return The parsed expression
     */
    public Expression parseExpression(String expressionString)
    {
        try
        {
            return spelExpressionParser.parseExpression(expressionString);
        }
        catch (ParseException e)
        {
            throw new IllegalArgumentException("Error parsing SpEL \"" + expressionString + "\"", e);
        }
    }

    /**
     * Evaluates the given expression using the given variables as context variables.
     * 
     * @param <T> The desired type
     * @param expression The expression to evaluate
     * @param desiredResultType The desired type of the result. Spring will make best attempt at converting result to this type.
     * @param variables The variables to put into context
     * @return The result of the evaluation
     */
    public <T> T evaluate(Expression expression, Class<T> desiredResultType, Map<String, Object> variables)
    {
        StandardEvaluationContext context = new StandardEvaluationContext();

        if (variables != null)
        {
            context.setVariables(variables);
        }

        return expression.getValue(context, desiredResultType);
    }

    /**
     * Evaluates the given expression using the given variables as context variables.
     * 
     * @param <T> The desired type
     * @param expression The expression to evaluate
     * @param variables The variables to put into context
     * @return The result of the evaluation
     */
    public <T> T evaluate(Expression expression, Map<String, Object> variables)
    {
        return evaluate(expression, null, variables);
    }
}
