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

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

import org.apache.maven.plugin.MojoExecutionException;

/**
 * Class that provides field utilities.
 */
public class FieldUtils
{
    /**
     * Gets the type of a collection field.
     *
     * @param field the collection field.
     *
     * @return the class type of the collection field.
     * @throws MojoExecutionException if any validation fails.
     */
    public static Class<?> getCollectionType(Field field) throws MojoExecutionException
    {
        // Get the class while performing validation.
        Type genericType = field.getGenericType();
        if (!(genericType instanceof ParameterizedType))
        {
            throw new MojoExecutionException("Field \"" + field.getName() + "\" is a collection that is not a ParameterizedType.");
        }
        ParameterizedType parameterizedType = (ParameterizedType) genericType;
        Type[] actualTypes = parameterizedType.getActualTypeArguments();
        if (actualTypes.length != 1)
        {
            throw new MojoExecutionException(
                "Field \"" + field.getName() + "\" is a collection that has " + actualTypes.length + " parameterized types and exactly 1 is required.");
        }
        Type actualType = actualTypes[0];
        if (!(actualType instanceof Class<?>))
        {
            throw new MojoExecutionException("Field \"" + field.getName() + "\" has a parameterized type which is not a class.");
        }
        return (Class<?>) actualType;
    }
}
