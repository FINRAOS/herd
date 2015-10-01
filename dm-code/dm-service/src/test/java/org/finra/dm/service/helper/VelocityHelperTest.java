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
package org.finra.dm.service.helper;

import java.util.HashMap;
import java.util.Map;

import org.finra.dm.service.AbstractServiceTest;
import org.junit.Assert;
import org.junit.Test;

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
}
