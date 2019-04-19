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
package org.finra.herd.service.config;

import java.lang.reflect.ReflectPermission;
import java.security.PrivilegedAction;
import java.util.Arrays;

import javax.script.Bindings;
import javax.script.ScriptEngineManager;

import org.activiti.engine.impl.scripting.ScriptBindingsFactory;
import org.activiti.engine.impl.scripting.ScriptingEngines;

import org.finra.herd.core.helper.SecurityManagerHelper;

/**
 * An implementation of {@link ScriptingEngines} which evaluates scripts with no permissions under a {@link SecurityManager}. Use this {@link ScriptingEngines}
 * implementation to restrict scripts that are sourced outside of the application. A security manager must be enabled for this JVM context for this
 * implementation to do anything meaningful. Otherwise, this implementation behaves similarly to {@link ScriptingEngines}.
 */
public class SecuredScriptingEngines extends ScriptingEngines
{
    public SecuredScriptingEngines(ScriptBindingsFactory scriptBindingsFactory)
    {
        super(scriptBindingsFactory);
    }

    public SecuredScriptingEngines(ScriptEngineManager scriptEngineManager)
    {
        super(scriptEngineManager);
    }

    /**
     * {@inheritDoc}
     * Executes the script under a script manager with no permissions.
     */
    @Override
    protected Object evaluate(final String script, final String language, final Bindings bindings)
    {
        return SecurityManagerHelper.doPrivileged(new PrivilegedAction<Object>()
        {
            @Override
            public Object run()
            {
                return SecuredScriptingEngines.super.evaluate(script, language, bindings);
            }
        }, Arrays
            .asList(new RuntimePermission("accessDeclaredMembers"),
                // Grants the permission to serialize/deserialize xml data type inside scripting engine
                new RuntimePermission("accessClassInPackage.com.sun.org.apache.xerces.internal.jaxp.datatype"),
                new ReflectPermission("suppressAccessChecks")));
    }
}
