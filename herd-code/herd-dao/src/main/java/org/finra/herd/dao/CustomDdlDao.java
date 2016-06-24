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
package org.finra.herd.dao;

import java.util.List;

import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.api.xml.CustomDdlKey;
import org.finra.herd.model.jpa.CustomDdlEntity;

public interface CustomDdlDao extends BaseJpaDao
{
    /**
     * Gets a custom DDL based on the key.
     *
     * @param customDdlKey the custom DDL key
     *
     * @return the custom DDL
     */
    public CustomDdlEntity getCustomDdlByKey(CustomDdlKey customDdlKey);

    /**
     * Gets the custom DDLs defined for the specified business object format.
     *
     * @param businessObjectFormatKey the business object format key
     *
     * @return the list of custom DDL keys
     */
    public List<CustomDdlKey> getCustomDdls(BusinessObjectFormatKey businessObjectFormatKey);
}
