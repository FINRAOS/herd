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

import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.cfg.MapperConfig;
import com.fasterxml.jackson.databind.introspect.AnnotatedField;
import com.fasterxml.jackson.databind.introspect.AnnotatedMethod;

/**
 * A custom naming strategy to handle the mapping of securityRequirement to security. This appears to be some type of bug in the model JSON field name given
 * that the YAML file seems to require "security" and not "securityRequirement" it to be correct in terms of the specification. This affects the herd-sdk code
 * generation which doesn't work for "basic auth" when the field name is incorrect.
 */
public class SwaggerNamingStrategy extends PropertyNamingStrategy
{
    private static final long serialVersionUID = 5638419660519839755L;

    @Override
    public String nameForField(MapperConfig config, AnnotatedField field, String defaultName)
    {
        return convertName(defaultName);
    }

    @Override
    public String nameForGetterMethod(MapperConfig config, AnnotatedMethod method, String defaultName)
    {
        return convertName(defaultName);
    }

    @Override
    public String nameForSetterMethod(MapperConfig config, AnnotatedMethod method, String defaultName)
    {
        return convertName(defaultName);
    }

    private String convertName(String name)
    {
        if (name.equalsIgnoreCase("securityRequirement"))
        {
            return "security";
        }
        else
        {
            return name;
        }
    }
}