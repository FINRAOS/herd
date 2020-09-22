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
package org.finra.herd.service.helper;

import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;

import org.apache.velocity.exception.MethodInvocationException;
import org.junit.Assert;
import org.junit.Test;

import org.finra.herd.service.AbstractServiceTest;

public class VelocityHelperTest extends AbstractServiceTest
{
    @Test
    public void testEvaluate()
    {
        String template = "${foo}";
        Map<String, Object> variables = new HashMap<>();
        variables.put("foo", "bar");
        String logTag = "test";
        String result = velocityHelper.evaluate(template, variables, logTag);
        Assert.assertEquals("result", "bar", result);
    }

    @Test
    public void testEvaluateReflection()
    {
        String template = "$foo.getClass().forName(\"java.lang.Runtime\").getRuntime().exec(\"cmd dir\")";
        Map<String, Object> variables = new HashMap<>();
        variables.put("foo", "bar");
        String logTag = "test";
        try
        {
            velocityHelper.evaluate(template, variables, logTag);
            fail();
        }
        catch (MethodInvocationException methodInvocationException)
        {
            Assert.assertEquals("Exception message not equal.",
                "Object 'java.lang.Class' does not contain method forName(java.lang.String) at test[line 1, column 17]",
                methodInvocationException.getMessage());
        }
    }
}