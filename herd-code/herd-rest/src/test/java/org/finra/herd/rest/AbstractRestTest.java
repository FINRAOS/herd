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

import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.web.WebAppConfiguration;

import org.finra.herd.rest.config.RestTestSpringModuleConfig;
import org.finra.herd.ui.AbstractUiTest;

/**
 * This is an abstract base class that provides useful methods for REST test drivers.
 */
@ContextConfiguration(classes = RestTestSpringModuleConfig.class, inheritLocations = false)
@WebAppConfiguration
public abstract class AbstractRestTest extends AbstractUiTest
{
}
