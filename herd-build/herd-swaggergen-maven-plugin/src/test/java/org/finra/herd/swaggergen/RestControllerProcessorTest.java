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
package org.finra.herd.swaggergen;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import io.swagger.models.Operation;
import io.swagger.models.Path;
import io.swagger.models.Swagger;
import org.junit.Test;

import org.finra.herd.swaggergen.test.definitionGenerator.BasicCase;

/**
 * Tests for RestControllerProcessor.
 * Normal operation of RestControllerProcessor requires a dependency to the jar containing the source.
 * Unit test cannot do that, therefore, any classes under test must also have its source under the resource under the same package structure.
 */
public class RestControllerProcessorTest extends AbstractTest
{
    @Test
    public void test_1() throws Exception
    {
        Swagger swagger = new Swagger();
        String restJavaPackage = "org.finra.herd.swaggergen.test.restControllerProcessor.case1";
        String tagPatternTemplate = "(?<tag>.+?)RestController";
        Class<?> modelErrorClass = BasicCase.class;
        RestControllerProcessor restControllerProcessor = new RestControllerProcessor(LOG, swagger, restJavaPackage, tagPatternTemplate, modelErrorClass);

        assertEquals(getResourceAsString("/yaml2.yaml"), toYaml(swagger));
        Set<String> exampleClassNames = restControllerProcessor.getExampleClassNames();
        assertNotNull(exampleClassNames);
        assertEquals(1, exampleClassNames.size());
        assertTrue(exampleClassNames.contains("BasicCase"));
    }

    @Test
    public void test_2() throws Exception
    {
        Swagger swagger = new Swagger();
        String restJavaPackage = "org.finra.herd.swaggergen.test.restControllerProcessor.case2";
        String tagPatternTemplate = "(?<tag>.+?)RestController";
        Class<?> modelErrorClass = null;
        new RestControllerProcessor(LOG, swagger, restJavaPackage, tagPatternTemplate, modelErrorClass);

        assertEquals(getResourceAsString("/yaml3.yaml"), toYaml(swagger));
    }

    @Test
    public void test_3() throws Exception
    {
        Swagger swagger = new Swagger();
        String restJavaPackage = "org.finra.herd.swaggergen.test.restControllerProcessor.case3";
        String tagPatternTemplate = "(?<tag>.+?)RestController";
        Class<?> modelErrorClass = null;
        new RestControllerProcessor(LOG, swagger, restJavaPackage, tagPatternTemplate, modelErrorClass);
        List<String> operationIds = new ArrayList<>();
        for (Path path : swagger.getPaths().values())
        {
            for (Operation operation : path.getOperations())
            {
                operationIds.add(operation.getOperationId());
            }
        }
        assertEquals(2, operationIds.size());
        assertTrue(operationIds.contains("Test3.duplicate"));
        assertTrue(operationIds.contains("Test3.duplicate1"));
    }
}
