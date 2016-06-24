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
package org.finra.herd.model.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.finra.herd.model.api.xml.NamespacePermissionEnum;

/**
 * Annotation to indicate that the namespace permission check should apply for the target method.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface NamespacePermission
{
    /**
     * List of SpEL expressions which indicate the field to inspect for the namespace value. The SpEL expression engine should be provided with all of the
     * method parameters as variables.
     * 
     * @return Array of SpEL expressions
     */
    String[] fields() default {};

    /**
     * List of permissions which the current user needs to the namespaces evaluated by the fields.
     * 
     * @return Array of permissions
     */
    NamespacePermissionEnum[] permissions() default {};
}
