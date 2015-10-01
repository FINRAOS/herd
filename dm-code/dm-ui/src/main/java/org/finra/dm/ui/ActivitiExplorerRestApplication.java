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

import org.activiti.rest.common.api.DefaultResource;
import org.activiti.rest.common.application.ActivitiRestApplication;
import org.activiti.rest.common.filter.JsonpFilter;
import org.activiti.rest.diagram.application.DiagramServicesInit;
import org.activiti.rest.editor.application.ModelerServicesInit;
import org.restlet.Restlet;
import org.restlet.routing.Router;

/**
 * Activiti Modeler embedded in Activiti Explorer UI needs the REST services. This class exposes the REST services that are used by Activiti Modeler to work.
 */
public class ActivitiExplorerRestApplication extends ActivitiRestApplication
{
    /**
     * Creates a root Restlet that will receive all incoming calls.
     */
    @Override
    public Restlet createInboundRoot()
    {
        synchronized (this)
        {
            Router router = new Router(getContext());
            router.attachDefault(DefaultResource.class);
            ModelerServicesInit.attachResources(router);
            DiagramServicesInit.attachResources(router);
            JsonpFilter jsonpFilter = new JsonpFilter(getContext());
            jsonpFilter.setNext(router);
            return jsonpFilter;
        }
    }
}