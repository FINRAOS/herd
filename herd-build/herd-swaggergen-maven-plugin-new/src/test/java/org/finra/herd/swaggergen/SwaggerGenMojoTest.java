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

import java.io.File;
import java.nio.file.Paths;
import java.util.Arrays;

import com.google.common.io.Files;
import org.apache.maven.plugin.MojoExecutionException;
import org.junit.Test;
import org.springframework.test.util.ReflectionTestUtils;

public class SwaggerGenMojoTest extends AbstractTest
{
    @Test
    public void test_1() throws Exception
    {
        File tempDir = Files.createTempDir();
        String outputFileName = "test.swagger.yaml";

        SwaggerGenMojo swaggerGenMojo = new SwaggerGenMojo();
        ReflectionTestUtils.setField(swaggerGenMojo, "outputDirectory", tempDir);
        ReflectionTestUtils.setField(swaggerGenMojo, "outputFilename", outputFileName);
        ReflectionTestUtils.setField(swaggerGenMojo, "restJavaPackage", "org.finra.herd.swaggergen.test.swaggerGenMojo.rest");
        ReflectionTestUtils.setField(swaggerGenMojo, "modelJavaPackage", "org.finra.herd.swaggergen.test.swaggerGenMojo.model");
        ReflectionTestUtils.setField(swaggerGenMojo, "modelErrorClassName", "ErrorResponse");
        ReflectionTestUtils.setField(swaggerGenMojo, "tagPatternParameter", "(?<tag>.+?)RestController");
        ReflectionTestUtils.setField(swaggerGenMojo, "title", "test_title");
        ReflectionTestUtils.setField(swaggerGenMojo, "version", "test_version");
        ReflectionTestUtils.setField(swaggerGenMojo, "basePath", "/test_basePath");
        ReflectionTestUtils.setField(swaggerGenMojo, "schemeParameters", Arrays.asList("http", "https"));
        swaggerGenMojo.execute();

        assertEquals(getResourceAsString("/yaml4.yaml"), getFileAsString(Paths.get(tempDir.getAbsolutePath(), outputFileName)));
    }

    @Test
    public void test_2() throws Exception
    {
        File tempDir = Files.createTempDir();
        tempDir = Paths.get(tempDir.toURI()).resolve("non_existent").toFile();
        String outputFileName = "swagger.yaml";

        SwaggerGenMojo swaggerGenMojo = new SwaggerGenMojo();
        ReflectionTestUtils.setField(swaggerGenMojo, "outputDirectory", tempDir);
        ReflectionTestUtils.setField(swaggerGenMojo, "outputFilename", outputFileName);
        ReflectionTestUtils.setField(swaggerGenMojo, "restJavaPackage", "org.finra.herd.swaggergen.test.swaggerGenMojo.rest");
        ReflectionTestUtils.setField(swaggerGenMojo, "modelJavaPackage", "org.finra.herd.swaggergen.test.swaggerGenMojo.model");
        ReflectionTestUtils.setField(swaggerGenMojo, "modelErrorClassName", "ErrorResponse");
        ReflectionTestUtils.setField(swaggerGenMojo, "tagPatternParameter", "(?<tag>.+?)RestController");
        ReflectionTestUtils.setField(swaggerGenMojo, "title", "test_title");
        ReflectionTestUtils.setField(swaggerGenMojo, "version", "test_version");
        ReflectionTestUtils.setField(swaggerGenMojo, "basePath", "/test_basePath");
        ReflectionTestUtils.setField(swaggerGenMojo, "schemeParameters", Arrays.asList("http", "https"));
        swaggerGenMojo.execute();

        assertEquals(getResourceAsString("/yaml4.yaml"), getFileAsString(Paths.get(tempDir.getAbsolutePath(), outputFileName)));
    }

    @Test
    public void test_3() throws Exception
    {
        File tempDir = Files.createTempDir();
        String outputFileName = "swagger.yaml";

        SwaggerGenMojo swaggerGenMojo = new SwaggerGenMojo();
        ReflectionTestUtils.setField(swaggerGenMojo, "outputDirectory", tempDir);
        ReflectionTestUtils.setField(swaggerGenMojo, "outputFilename", outputFileName);
        ReflectionTestUtils.setField(swaggerGenMojo, "restJavaPackage", "org.finra.herd.swaggergen.test.swaggerGenMojo.rest");
        ReflectionTestUtils.setField(swaggerGenMojo, "modelJavaPackage", "org.finra.herd.swaggergen.test.swaggerGenMojo.model");
        ReflectionTestUtils.setField(swaggerGenMojo, "modelErrorClassName", "ErrorResponse");
        ReflectionTestUtils.setField(swaggerGenMojo, "tagPatternParameter", "(?<tag>.+?)RestController");
        ReflectionTestUtils.setField(swaggerGenMojo, "title", "test_title");
        ReflectionTestUtils.setField(swaggerGenMojo, "version", "test_version");
        ReflectionTestUtils.setField(swaggerGenMojo, "basePath", "/test_basePath");
        swaggerGenMojo.execute();

        assertEquals(getResourceAsString("/yaml5.yaml"), getFileAsString(Paths.get(tempDir.getAbsolutePath(), outputFileName)));
    }

    @Test
    public void test_4() throws Exception
    {
        File tempDir = Files.createTempDir();
        String outputFileName = "swagger.yaml";

        SwaggerGenMojo swaggerGenMojo = new SwaggerGenMojo();
        ReflectionTestUtils.setField(swaggerGenMojo, "outputDirectory", tempDir);
        ReflectionTestUtils.setField(swaggerGenMojo, "outputFilename", outputFileName);
        ReflectionTestUtils.setField(swaggerGenMojo, "restJavaPackage", "org.finra.herd.swaggergen.test.swaggerGenMojo.rest");
        ReflectionTestUtils.setField(swaggerGenMojo, "modelJavaPackage", "org.finra.herd.swaggergen.test.swaggerGenMojo.model");
        ReflectionTestUtils.setField(swaggerGenMojo, "modelErrorClassName", "ErrorResponse");
        ReflectionTestUtils.setField(swaggerGenMojo, "tagPatternParameter", "(?<tag>.+?)RestController");
        ReflectionTestUtils.setField(swaggerGenMojo, "title", "test_title");
        ReflectionTestUtils.setField(swaggerGenMojo, "version", "test_version");
        ReflectionTestUtils.setField(swaggerGenMojo, "basePath", "/test_basePath");
        ReflectionTestUtils.setField(swaggerGenMojo, "schemeParameters", Arrays.asList("http", null));
        try
        {
            swaggerGenMojo.execute();
            fail();
        }
        catch (Exception e)
        {
            assertEquals(MojoExecutionException.class, e.getClass());
            assertEquals("Invalid scheme specified: null", e.getMessage());
        }
    }

    @Test
    public void test_5() throws Exception
    {
        File tempDir = Files.createTempDir();
        String outputFileName = "swagger.yaml";

        SwaggerGenMojo swaggerGenMojo = new SwaggerGenMojo();
        ReflectionTestUtils.setField(swaggerGenMojo, "outputDirectory", tempDir);
        ReflectionTestUtils.setField(swaggerGenMojo, "outputFilename", outputFileName);
        ReflectionTestUtils.setField(swaggerGenMojo, "restJavaPackage", "org.finra.herd.swaggergen.test.swaggerGenMojo.rest");
        ReflectionTestUtils.setField(swaggerGenMojo, "modelJavaPackage", "org.finra.herd.swaggergen.test.swaggerGenMojo.model");
        ReflectionTestUtils.setField(swaggerGenMojo, "modelErrorClassName", "ErrorResponse");
        ReflectionTestUtils.setField(swaggerGenMojo, "tagPatternParameter", "(?<tag>.+?)RestController");
        ReflectionTestUtils.setField(swaggerGenMojo, "title", "test_title");
        ReflectionTestUtils.setField(swaggerGenMojo, "version", "test_version");
        ReflectionTestUtils.setField(swaggerGenMojo, "basePath", "/test_basePath");
        ReflectionTestUtils.setField(swaggerGenMojo, "schemeParameters", Arrays.asList("http", "https"));
        ReflectionTestUtils.setField(swaggerGenMojo, "xsdName", "xsd3.xsd");
        swaggerGenMojo.execute();

        assertEquals(getResourceAsString("/yaml6.yaml"), getFileAsString(Paths.get(tempDir.getAbsolutePath(), outputFileName)));
    }

    @Test
    public void test_GetOperationsFilterShouldBeApplied() throws Exception
    {
        File tempDir = Files.createTempDir();
        String outputFileName = "swagger.yaml";

        SwaggerGenMojo swaggerGenMojo = new SwaggerGenMojo();
        ReflectionTestUtils.setField(swaggerGenMojo, "outputDirectory", tempDir);
        ReflectionTestUtils.setField(swaggerGenMojo, "outputFilename", outputFileName);
        ReflectionTestUtils.setField(swaggerGenMojo, "restJavaPackage", "org.finra.herd.swaggergen.test.swaggerGenMojo.rest");
        ReflectionTestUtils.setField(swaggerGenMojo, "modelJavaPackage", "org.finra.herd.swaggergen.test.swaggerGenMojo.model");
        ReflectionTestUtils.setField(swaggerGenMojo, "modelErrorClassName", "ErrorResponse");
        ReflectionTestUtils.setField(swaggerGenMojo, "tagPatternParameter", "(?<tag>.+?)RestController");
        ReflectionTestUtils.setField(swaggerGenMojo, "title", "test_title");
        ReflectionTestUtils.setField(swaggerGenMojo, "version", "test_version");
        ReflectionTestUtils.setField(swaggerGenMojo, "basePath", "/test_basePath");
        ReflectionTestUtils.setField(swaggerGenMojo, "schemeParameters", Arrays.asList("http", "https"));
        ReflectionTestUtils.setField(swaggerGenMojo, "applyOperationsFilter", true);
        ReflectionTestUtils.setField(swaggerGenMojo, "includeOperations", new String[] {"Person.get"});

        swaggerGenMojo.execute();

        assertEquals(getResourceAsString("/yaml_GetOperationsFilter.yaml"), getFileAsString(Paths.get(tempDir.getAbsolutePath(), outputFileName)));
    }
}
