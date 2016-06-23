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

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import org.finra.herd.core.AbstractCoreTest;

import static org.junit.Assert.*;

public class SpelExpressionHelperTest extends AbstractCoreTest
{
    @Autowired
    private SpelExpressionHelper spelExpressionHelper;

    @Test
    public void evaluateAssertNoErrorsOnValidExpression()
    {
        String expressionString = "#foo";
        Map<String, Object> variables = new HashMap<>();
        variables.put("foo", "bar");
        assertEquals("bar", spelExpressionHelper.evaluate(expressionString, variables));
    }

    @Test
    public void evaluateAssertIllegalArgumentOnInvalidExpression()
    {
        String expressionString = "this is an invalid expression";
        try
        {
            spelExpressionHelper.evaluate(expressionString, null);
            fail();
        }
        catch (Exception e)
        {
            assertEquals(IllegalArgumentException.class, e.getClass());
            assertEquals("Error parsing SpEL \"this is an invalid expression\"", e.getMessage());
        }
    }
}
