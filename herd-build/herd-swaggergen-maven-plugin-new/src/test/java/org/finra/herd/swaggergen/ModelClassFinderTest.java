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
import static org.junit.Assert.assertTrue;

import java.util.Set;

import org.apache.maven.plugin.MojoExecutionException;
import org.junit.Test;

import org.finra.herd.swaggergen.test.modelClassFinder.Bar;
import org.finra.herd.swaggergen.test.modelClassFinder.Foo;

/**
 * Tests for ModelClassFinder.
 */
public class ModelClassFinderTest extends AbstractTest
{
    /**
     * Given package exists
     * When package is specifies, error class is specified
     * Assert classes found
     * 
     * @throws MojoExecutionException
     */
    @Test
    public void test_1() throws MojoExecutionException
    {
        ModelClassFinder modelClassFinder = new ModelClassFinder(LOG, "org.finra.herd.swaggergen.test.modelClassFinder", "Foo");
        Set<Class<?>> modelClasses = modelClassFinder.getModelClasses();
        Class<?> modelErrorClass = modelClassFinder.getModelErrorClass();
        assertEquals(2, modelClasses.size());
        assertTrue(modelClasses.contains(Foo.class));
        assertTrue(modelClasses.contains(Bar.class));
        assertEquals(Foo.class, modelErrorClass);
    }
}
