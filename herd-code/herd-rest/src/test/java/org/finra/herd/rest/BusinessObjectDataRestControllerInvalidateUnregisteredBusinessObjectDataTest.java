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

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import org.finra.herd.model.api.xml.BusinessObjectDataInvalidateUnregisteredRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataInvalidateUnregisteredResponse;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;

public class BusinessObjectDataRestControllerInvalidateUnregisteredBusinessObjectDataTest extends AbstractRestTest
{
    @Test
    public void test()
    {
        BusinessObjectDataInvalidateUnregisteredRequest businessObjectDataInvalidateUnregisteredRequest =
            getDefaultBusinessObjectDataInvalidateUnregisteredRequest();
        BusinessObjectFormatEntity businessObjectFormatEntity = createBusinessObjectFormat(businessObjectDataInvalidateUnregisteredRequest);
        createS3Object(businessObjectFormatEntity, businessObjectDataInvalidateUnregisteredRequest, 0);
        BusinessObjectDataInvalidateUnregisteredResponse businessObjectDataInvalidateUnregisteredResponse = businessObjectDataRestController
            .invalidateUnregisteredBusinessObjectData(businessObjectDataInvalidateUnregisteredRequest);

        assertEquals(businessObjectDataInvalidateUnregisteredRequest.getNamespace(), businessObjectDataInvalidateUnregisteredResponse.getNamespace());
        assertEquals(businessObjectDataInvalidateUnregisteredRequest.getBusinessObjectDefinitionName(), businessObjectDataInvalidateUnregisteredResponse
            .getBusinessObjectDefinitionName());
        assertEquals(businessObjectDataInvalidateUnregisteredRequest.getBusinessObjectFormatUsage(), businessObjectDataInvalidateUnregisteredResponse
            .getBusinessObjectFormatUsage());
        assertEquals(businessObjectDataInvalidateUnregisteredRequest.getBusinessObjectFormatFileType(), businessObjectDataInvalidateUnregisteredResponse
            .getBusinessObjectFormatFileType());
        assertEquals(businessObjectDataInvalidateUnregisteredRequest.getBusinessObjectFormatVersion(), businessObjectDataInvalidateUnregisteredResponse
            .getBusinessObjectFormatVersion());
        assertEquals(businessObjectDataInvalidateUnregisteredRequest.getPartitionValue(), businessObjectDataInvalidateUnregisteredResponse.getPartitionValue());
        assertEquals(businessObjectDataInvalidateUnregisteredRequest.getStorageName(), businessObjectDataInvalidateUnregisteredResponse.getStorageName());
        assertEquals(1, businessObjectDataInvalidateUnregisteredResponse.getRegisteredBusinessObjectDataList().size());
    }
}
