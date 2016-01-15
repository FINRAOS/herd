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
import static org.junit.Assert.fail;

import org.activiti.engine.delegate.Expression;
import org.activiti.engine.impl.el.FixedValue;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import org.finra.herd.service.AbstractServiceTest;

/**
 * Tests the ActivitiHelper class.
 */
public class ActivitiHelperTest extends AbstractServiceTest
{

    @Autowired
    protected ActivitiHelper activitiHelper;

    @Test
    public void testGetExpressionVariableAsIntegerRequiredError()
    {
        Expression fixedValue = new FixedValue("");

        try
        {
            activitiHelper.getExpressionVariableAsInteger(fixedValue, null, "fixedValue", true);
            fail("Should throw an IllegalArgumentException.");
        }
        catch (IllegalArgumentException ex)
        {
            assertEquals("\"fixedValue\" must be specified.", ex.getMessage());
        }
    }

    @Test
    public void testGetExpressionVariableAsBooleanRequiredError()
    {
        Expression fixedValue = new FixedValue("");

        try
        {
            activitiHelper.getExpressionVariableAsBoolean(fixedValue, null, "fixedValue", true, false);
            fail("Should throw an IllegalArgumentException.");
        }
        catch (IllegalArgumentException ex)
        {
            assertEquals("\"fixedValue\" must be specified.", ex.getMessage());
        }
    }
}