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
package org.finra.dm.ui;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.web.WebAppConfiguration;

import org.finra.dm.service.AbstractServiceTest;
import org.finra.dm.ui.config.UiTestSpringModuleConfig;
import org.finra.dm.ui.controller.DmController;

/**
 * This is an abstract base class that provides useful methods for UI test drivers. We need to use a customized loader that stores the web application context
 * in an application context holder so static bean creation methods don't fail. This is similar to what WarInitializer.java does for the WAR, but rather for the
 * JUnits. A special WebApplicationContext is required when the @WebAppConfiguration annotation is present. See @WebAppConfiguration for more information.
 */
@ContextConfiguration(classes = UiTestSpringModuleConfig.class, inheritLocations = false, loader = WebContextHolderContextLoader.class)
@WebAppConfiguration
public abstract class AbstractUiTest extends AbstractServiceTest
{
    // Provide easy access to the UI controller for all test methods.
    @Autowired
    protected DmController dmController;
}
