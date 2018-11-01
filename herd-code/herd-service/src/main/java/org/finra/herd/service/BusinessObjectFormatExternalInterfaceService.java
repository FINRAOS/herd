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

import org.finra.herd.model.api.xml.BusinessObjectFormatExternalInterface;
import org.finra.herd.model.api.xml.BusinessObjectFormatExternalInterfaceCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectFormatExternalInterfaceKey;

/**
 * The business object format to external interface mapping service.
 */
public interface BusinessObjectFormatExternalInterfaceService
{
    /**
     * Creates a new business object format to external interface mapping.
     *
     * @param businessObjectFormatExternalInterfaceCreateRequest the information needed to create a business object format to external interface mapping
     *
     * @return the created business object format to external interface mapping
     */
    BusinessObjectFormatExternalInterface createBusinessObjectFormatExternalInterface(
        BusinessObjectFormatExternalInterfaceCreateRequest businessObjectFormatExternalInterfaceCreateRequest);

    /**
     * Deletes an existing business object format to external interface mapping by its key.
     *
     * @param businessObjectFormatExternalInterfaceKey the business object format to external interface mapping key
     *
     * @return the deleted business object format to external interface mapping
     */
    BusinessObjectFormatExternalInterface deleteBusinessObjectFormatExternalInterface(
        BusinessObjectFormatExternalInterfaceKey businessObjectFormatExternalInterfaceKey);

    /**
     * Retrieves an existing business object format to external interface mapping by its key.
     *
     * @param businessObjectFormatExternalInterfaceKey the business object format to external interface mapping key
     *
     * @return the retrieved business object format to external interface mapping
     */
    BusinessObjectFormatExternalInterface getBusinessObjectFormatExternalInterface(
        BusinessObjectFormatExternalInterfaceKey businessObjectFormatExternalInterfaceKey);
}
