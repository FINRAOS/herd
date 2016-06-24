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
package org.finra.herd.service.advice;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.stereotype.Component;

import org.finra.herd.core.helper.SpelExpressionHelper;
import org.finra.herd.model.annotation.NamespacePermission;
import org.finra.herd.model.annotation.NamespacePermissions;
import org.finra.herd.service.helper.NamespaceSecurityHelper;

@Component
@Aspect
public class NamespaceSecurityAdvice extends AbstractServiceAdvice
{
    @Autowired
    private SpelExpressionHelper spelExpressionHelper;

    @Autowired
    private NamespaceSecurityHelper namespaceSecurityHelper;

    /**
     * Check permission on the service methods before the execution. The method is expected to throw AccessDeniedException if current user does not have the
     * permissions.
     * 
     * @param joinPoint The join point
     */
    @Before("serviceMethods()")
    public void checkPermission(JoinPoint joinPoint)
    {

        MethodSignature methodSignature = (MethodSignature) joinPoint.getSignature();
        Method method = methodSignature.getMethod();

        List<NamespacePermission> namespacePermissions = new ArrayList<>();
        if (method.isAnnotationPresent(NamespacePermissions.class))
        {
            namespacePermissions.addAll(Arrays.asList(method.getAnnotation(NamespacePermissions.class).value()));
        }
        else if (method.isAnnotationPresent(NamespacePermission.class))
        {
            namespacePermissions.add(method.getAnnotation(NamespacePermission.class));
        }

        if (!namespacePermissions.isEmpty())
        {
            String[] parameterNames = methodSignature.getParameterNames();
            Object[] args = joinPoint.getArgs();

            Map<String, Object> variables = new HashMap<>();
            for (int i = 0; i < parameterNames.length; i++)
            {
                variables.put(parameterNames[i], args[i]);
            }

            List<AccessDeniedException> accessDeniedExceptions = new ArrayList<>();
            for (NamespacePermission namespacePermission : namespacePermissions)
            {
                for (String field : namespacePermission.fields())
                {
                    try
                    {
                        namespaceSecurityHelper.checkPermission(spelExpressionHelper.evaluate(field, Object.class, variables), namespacePermission
                            .permissions());
                    }
                    catch (AccessDeniedException accessDeniedException)
                    {
                        accessDeniedExceptions.add(accessDeniedException);
                    }
                }
            }
            if (!accessDeniedExceptions.isEmpty())
            {
                throw namespaceSecurityHelper.getAccessDeniedException(accessDeniedExceptions);
            }
        }
    }
}
