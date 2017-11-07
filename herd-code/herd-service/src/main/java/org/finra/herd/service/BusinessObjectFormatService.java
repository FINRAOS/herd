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

import org.finra.herd.model.api.xml.*;

/**
 * The business object definition format.
 */
public interface BusinessObjectFormatService
{
    public BusinessObjectFormat createBusinessObjectFormat(BusinessObjectFormatCreateRequest businessObjectFormatCreateRequest);

    public BusinessObjectFormat updateBusinessObjectFormat(BusinessObjectFormatKey businessObjectFormatKey,
        BusinessObjectFormatUpdateRequest businessObjectFormatUpdateRequest);

    public BusinessObjectFormat getBusinessObjectFormat(BusinessObjectFormatKey businessObjectFormatKey);

    public BusinessObjectFormat deleteBusinessObjectFormat(BusinessObjectFormatKey businessObjectFormatKey);

    public BusinessObjectFormatKeys getBusinessObjectFormats(BusinessObjectDefinitionKey businessObjectDefinitionKey,
        boolean latestBusinessObjectFormatVersion);

    public BusinessObjectFormatDdl generateBusinessObjectFormatDdl(BusinessObjectFormatDdlRequest businessObjectFormatDdlRequest);

    public BusinessObjectFormatDdlCollectionResponse generateBusinessObjectFormatDdlCollection(
        BusinessObjectFormatDdlCollectionRequest businessObjectFormatDdlCollectionRequest);

    public BusinessObjectFormat updateBusinessObjectFormatParents(BusinessObjectFormatKey businessObjectFormatKey,
        BusinessObjectFormatParentsUpdateRequest businessObjectFormatParentsUpdateRequest);

    public BusinessObjectFormat updateBusinessObjectFormatAttributes(BusinessObjectFormatKey businessObjectFormatKey,
        BusinessObjectFormatAttributesUpdateRequest businessObjectFormatAttributesUpdateRequest);

    public BusinessObjectFormat updateBusinessObjectFormatAttributeDefinitions(BusinessObjectFormatKey businessObjectFormatKey,
        BusinessObjectFormatAttributeDefinitionsUpdateRequest businessObjectFormatAttributeDefinitionsUpdateRequest);

    public BusinessObjectFormat updateBusinessObjectFormatRetentionInformation(BusinessObjectFormatKey businessObjectFormatKey,
        BusinessObjectFormatRetentionInformationUpdateRequest businessObjectFormatRetentionInformationUpdateRequest);
}
