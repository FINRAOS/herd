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
package org.finra.herd.service.helper;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import org.finra.herd.model.api.xml.BusinessObjectDataSearchKey;
import org.finra.herd.model.api.xml.BusinessObjectDataSearchRequest;

/*
 * a helper class Business Object Data Search 
 */
@Component
public class BusinessObjectDataSearchHelper
{
    @Autowired
    private AlternateKeyHelper alternateKeyHelper;

    /**
     * validate business object search request
     *
     * @param request business object data search key
     *
     * @throws IllegalArgumentException when business object data search request is not valid
     */
    public void validBusinesObjectDataSearchRequest(BusinessObjectDataSearchRequest request) throws IllegalArgumentException
    {
        Assert.isTrue(request != null, "BusinessObjectDataSearchRequest should not be null");
        Assert.isTrue(request.getBusinessObjectDataSearchFilters() != null, "BusinessObjectDataSearchFilters is required");
        Assert.isTrue(request.getBusinessObjectDataSearchFilters().size() == 1, "BusinessObjectDataSearchFilters can only have one filter for now");
        Assert.isTrue(request.getBusinessObjectDataSearchFilters().get(0).getBusinessObjectDataSearchKeys() != null, "BusinessObject Search Key is required");

        List<BusinessObjectDataSearchKey> searchKeyList = request.getBusinessObjectDataSearchFilters().get(0).getBusinessObjectDataSearchKeys();
        Assert.isTrue(searchKeyList.size() == 1, "BusinessObject Search Key can only have one for now");

        for (BusinessObjectDataSearchKey key : searchKeyList)
        {
            validateBusinessObjectDataKey(key);
        }
    }

    /**
     * validate business search key
     *
     * @param key business data search key
     *
     * @throws IllegalArgumentException when business object data search key is not valid
     */
    public void validateBusinessObjectDataKey(BusinessObjectDataSearchKey key) throws IllegalArgumentException
    {
        Assert.notNull(key, "A business object data key must be specified.");
        key.setNamespace(alternateKeyHelper.validateStringParameter("namespace", key.getNamespace()));
        key.setBusinessObjectDefinitionName(
            alternateKeyHelper.validateStringParameter("business object definition name", key.getBusinessObjectDefinitionName()));

        if (key.getBusinessObjectFormatUsage() != null)
        {
            key.setBusinessObjectFormatUsage(alternateKeyHelper.validateStringParameter("business object format usage", key.getBusinessObjectFormatUsage()));
        }

        if (key.getBusinessObjectFormatFileType() != null)
        {
            key.setBusinessObjectFormatFileType(
                alternateKeyHelper.validateStringParameter("business object format file type", key.getBusinessObjectFormatFileType()));
        }
    }
}
