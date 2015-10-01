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
package org.finra.dm.core;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.Signature;
import org.aspectj.lang.reflect.SourceLocation;
import org.aspectj.runtime.internal.AroundClosure;

/**
 * This is a mock proceeding join point that can be used to aid in JUnit test drivers that test advice classes. It has no direct functionality. Sub-classes can
 * extend this implementation to provide more specific mock functionality.
 */
public class MockProceedingJoinPoint implements ProceedingJoinPoint
{
    @Override
    public Object proceed() throws Throwable
    {
        return this;
    }

    @Override
    public void set$AroundClosure(AroundClosure arc)
    {
    }

    @Override
    public Object proceed(Object[] args) throws Throwable
    {
        return this;
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
    public Object getThis()
    {
        return this;
    }

    @Override
    public Object getTarget()
    {
        return this;
    }

    @Override
    public Object[] getArgs()
    {
        return new Object[0];
    }

    @Override
    public Signature getSignature()
    {
        return new MockMethodSignature();
    }

    @Override
    public SourceLocation getSourceLocation()
    {
        return null;
    }

    @Override
    public String getKind()
    {
        return null;
    }

    @Override
    public JoinPoint.StaticPart getStaticPart()
    {
        return null;
    }
}
