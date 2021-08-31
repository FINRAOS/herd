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
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

import io.swagger.models.Model;
import io.swagger.models.Swagger;
import org.apache.maven.plugin.MojoExecutionException;
import org.junit.Test;

import org.finra.herd.swaggergen.test.NonXmlType;
import org.finra.herd.swaggergen.test.definitionGenerator.BasicCase;
import org.finra.herd.swaggergen.test.definitionGenerator.MultiTypedListCase;
import org.finra.herd.swaggergen.test.definitionGenerator.NonParametrizedListCase;
import org.finra.herd.swaggergen.test.definitionGenerator.TrivialCase;
import org.finra.herd.swaggergen.test.definitionGenerator.WildcardParameterizedListCase;
import org.finra.herd.swaggergen.test.definitionGenerator.XsdMappingCase;

public class DefinitionGeneratorTest extends AbstractTest
{
    @Test
    public void test_1() throws Exception
    {
        Swagger swagger = new Swagger();
        swagger.setDefinitions(new HashMap<>());
        Set<String> exampleClassNames = new HashSet<>(Arrays.asList("BasicCase"));
        Set<Class<?>> modelClasses = new LinkedHashSet<>(Arrays.asList(BasicCase.class, TrivialCase.class, NonXmlType.class));
        new DefinitionGenerator(LOG, swagger, exampleClassNames, modelClasses, null);

        assertEquals(getResourceAsString("/yaml1.yaml"), toYaml(swagger));
    }

    @Test
    public void test_2() throws Exception
    {
        Swagger swagger = new Swagger();
        swagger.setDefinitions(new HashMap<>());
        Set<String> exampleClassNames = new HashSet<>();
        Set<Class<?>> modelClasses = new HashSet<>(Arrays.asList(NonParametrizedListCase.class));
        try
        {
            new DefinitionGenerator(LOG, swagger, exampleClassNames, modelClasses, null);
            fail();
        }
        catch (Exception e)
        {
            assertEquals(MojoExecutionException.class, e.getClass());
            assertEquals("Field \"list\" is a collection that is not a ParameterizedType.", e.getMessage());
        }
    }

    @Test
    public void test_3() throws Exception
    {
        Swagger swagger = new Swagger();
        swagger.setDefinitions(new HashMap<>());
        Set<String> exampleClassNames = new HashSet<>();
        Set<Class<?>> modelClasses = new HashSet<>(Arrays.asList(WildcardParameterizedListCase.class));
        try
        {
            new DefinitionGenerator(LOG, swagger, exampleClassNames, modelClasses, null);
            fail();
        }
        catch (Exception e)
        {
            assertEquals(MojoExecutionException.class, e.getClass());
            assertEquals("Field \"list\" has a parameterized type which is not a class.", e.getMessage());
        }
    }

    @Test
    public void test_4() throws Exception
    {
        Swagger swagger = new Swagger();
        swagger.setDefinitions(new HashMap<>());
        Set<String> exampleClassNames = new HashSet<>();
        Set<Class<?>> modelClasses = new HashSet<>(Arrays.asList(MultiTypedListCase.class));
        try
        {
            new DefinitionGenerator(LOG, swagger, exampleClassNames, modelClasses, null);
            fail();
        }
        catch (Exception e)
        {
            assertEquals(MojoExecutionException.class, e.getClass());
            assertEquals("Field \"list\" is a collection that has 2 parameterized types and exactly 1 is required.", e.getMessage());
        }
    }

    @Test
    public void test_5() throws Exception
    {
        Swagger swagger = new Swagger();
        swagger.setDefinitions(new HashMap<>());
        Set<String> exampleClassNames = new HashSet<>();
        Set<Class<?>> modelClasses = new HashSet<>(Arrays.asList(XsdMappingCase.class));
        new DefinitionGenerator(LOG, swagger, exampleClassNames, modelClasses, new XsdParser("xsd2.xsd"));
        Model definition = swagger.getDefinitions().get("xsdMappingCase");
        assertEquals("The xsdMappingCase", definition.getDescription());
        assertEquals("The scalar1", definition.getProperties().get("scalar1").getDescription());
        assertEquals("The scalar2", definition.getProperties().get("scalar2").getDescription());
    }
}
