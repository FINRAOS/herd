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
package org.finra.dm.service;

import org.finra.dm.model.api.xml.BusinessObjectFormatKey;
import org.finra.dm.model.api.xml.CustomDdl;
import org.finra.dm.model.api.xml.CustomDdlCreateRequest;
import org.finra.dm.model.api.xml.CustomDdlKey;
import org.finra.dm.model.api.xml.CustomDdlKeys;
import org.finra.dm.model.api.xml.CustomDdlUpdateRequest;

/**
 * The custom DDL service.
 */
public interface CustomDdlService
{
    public CustomDdl createCustomDdl(CustomDdlCreateRequest customDdlCreateRequest);

    public CustomDdl getCustomDdl(CustomDdlKey customDdlKey);

    public CustomDdl updateCustomDdl(CustomDdlKey customDdlKey, CustomDdlUpdateRequest customDdlUpdateRequest);

    public CustomDdl deleteCustomDdl(CustomDdlKey customDdlKey);

    public CustomDdlKeys getCustomDdls(BusinessObjectFormatKey businessObjectFormatKey);
}
