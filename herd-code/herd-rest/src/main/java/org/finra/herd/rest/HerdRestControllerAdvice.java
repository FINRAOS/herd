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
package org.finra.herd.rest;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.web.bind.annotation.ControllerAdvice;

import org.finra.herd.service.helper.HerdErrorInformationExceptionHandler;

/**
 * A general REST controller advice that can be used to handle REST controller exceptions. All the core functionality is in the base class. This class is needed
 * to define the functionality as a controller advice that is only used within the "REST" code.
 */
@ControllerAdvice("org.finra.herd.rest")
// Only handle REST exceptions and not UI exceptions which are defined in a different Java root package.
public class HerdRestControllerAdvice extends HerdErrorInformationExceptionHandler implements InitializingBean
{
    @Override
    public void afterPropertiesSet() throws Exception
    {
        // When using this bean as a controller advice, we want logging enabled as opposed to when the base class bean is used (not as advice) where we want
        // logging disabled.
        setLoggingEnabled(true);
    }
}
