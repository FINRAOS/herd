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
package org.finra.herd.core;

import java.lang.reflect.Method;

import org.aspectj.lang.reflect.MethodSignature;

/**
 * A mock implementation of a method signature.
 */
public class MockMethodSignature implements MethodSignature
{
    @Override
    public Class getReturnType()
    {
        return this.getClass();
    }

    /**
     * Returns a handle to this method.
     *
     * @return a handle to this method.
     */
    @Override
    public Method getMethod()
    {
        try
        {
            // Return a handle to this method.
            return this.getClass().getMethod("getMethod");
        }
        catch (Exception ex)
        {
            // We shouldn't get here since the method we're returning is this method.
            return null;
        }
    }

    @Override
    public Class[] getParameterTypes()
    {
        return new Class[0];
    }

    @Override
    public String[] getParameterNames()
    {
        return new String[0];
    }

    @Override
    public Class[] getExceptionTypes()
    {
        return new Class[0];
    }

    @Override
    public String toShortString()
    {
        return this.toString();
    }

    @Override
    public String toLongString()
    {
        return this.toString();
    }

    @Override
    public String getName()
    {
        return this.toString();
    }

    @Override
    public int getModifiers()
    {
        return 0;
    }

    @Override
    public Class getDeclaringType()
    {
        return this.getClass();
    }

    @Override
    public String getDeclaringTypeName()
    {
        return this.toString();
    }
}
