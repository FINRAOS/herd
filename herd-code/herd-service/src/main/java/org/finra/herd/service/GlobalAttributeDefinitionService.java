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
package org.finra.herd.service;

import org.finra.herd.model.api.xml.GlobalAttributeDefinition;
import org.finra.herd.model.api.xml.GlobalAttributeDefinitionCreateRequest;
import org.finra.herd.model.api.xml.GlobalAttributeDefinitionKey;
import org.finra.herd.model.api.xml.GlobalAttributeDefinitionKeys;

/**
 * The global attribute definition service.
 */
public interface GlobalAttributeDefinitionService
{
    /**
     * Creates a new global attribute definition.
     *
     * @param request the global attribute definition create request
     *
     * @return the created global attribute definition
     */
    public GlobalAttributeDefinition createGlobalAttributeDefinition(GlobalAttributeDefinitionCreateRequest request);

    /**
     * Deletes a global attribute definition.
     *
     * @param globalAttributeDefinitionKey the global attribute definition key
     *
     * @return the deleted global attribute definition
     */
    public GlobalAttributeDefinition deleteGlobalAttributeDefinition(GlobalAttributeDefinitionKey globalAttributeDefinitionKey);


    /**
     * Retrieves a list of global attribute definition keys.
     *
     * @return the list of retrieved global attribute definition keys
     */
    public GlobalAttributeDefinitionKeys getGlobalAttributeDefinitionKeys();

    /**
     * Returns a global attribute definition.
     * 
     * @param globalAttributeDefinitionKey the global attribute definition key
     * 
     * @return the global attribute definition
     */
    public GlobalAttributeDefinition getGlobalAttributeDefinition(GlobalAttributeDefinitionKey globalAttributeDefinitionKey);

}
