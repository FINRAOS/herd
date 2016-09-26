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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.dao.BusinessObjectDataDaoTestHelper;
import org.finra.herd.model.api.xml.BusinessObjectDataAttribute;
import org.finra.herd.model.api.xml.BusinessObjectDataAttributeCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataAttributeKey;
import org.finra.herd.model.api.xml.BusinessObjectDataAttributeUpdateRequest;

@Component
public class BusinessObjectDataAttributeServiceTestHelper
{
    @Autowired
    private BusinessObjectDataDaoTestHelper businessObjectDataDaoTestHelper;

    /**
     * Creates a business object data attribute create request.
     *
     * @return the newly created business object data attribute create request
     */
    public BusinessObjectDataAttributeCreateRequest createBusinessObjectDataAttributeCreateRequest(String namespaceCode, String businessObjectDefinitionName,
        String businessObjectFormatUsage, String businessObjectFormatFileType, Integer businessObjectFormatVersion, String businessObjectDataPartitionValue,
        List<String> businessObjectDataSubPartitionValues, Integer businessObjectDataVersion, String businessObjectDataAttributeName,
        String businessObjectDataAttributeValue)
    {
        BusinessObjectDataAttributeCreateRequest request = new BusinessObjectDataAttributeCreateRequest();

        request.setBusinessObjectDataAttributeKey(
            new BusinessObjectDataAttributeKey(namespaceCode, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, businessObjectDataPartitionValue, businessObjectDataSubPartitionValues, businessObjectDataVersion,
                businessObjectDataAttributeName));
        request.setBusinessObjectDataAttributeValue(businessObjectDataAttributeValue);

        return request;
    }

    /**
     * Creates a business object data attribute update request.
     *
     * @return the newly created business object data attribute update request
     */
    public BusinessObjectDataAttributeUpdateRequest createBusinessObjectDataAttributeUpdateRequest(String businessObjectDataAttributeValue)
    {
        BusinessObjectDataAttributeUpdateRequest request = new BusinessObjectDataAttributeUpdateRequest();

        request.setBusinessObjectDataAttributeValue(businessObjectDataAttributeValue);

        return request;
    }

    /**
     * Validates business object data attribute contents against specified arguments.
     *
     * @param businessObjectDataAttributeId the expected business object data attribute ID
     * @param expectedNamespace the expected namespace
     * @param expectedBusinessObjectDefinitionName the expected business object definition name
     * @param expectedBusinessObjectFormatUsage the expected business object format usage
     * @param expectedBusinessObjectFormatFileType the expected business object format file type
     * @param expectedBusinessObjectFormatVersion the expected business object format version
     * @param expectedBusinessObjectDataPartitionValue the expected partition value
     * @param expectedBusinessObjectDataSubPartitionValues the expected subpartition values
     * @param expectedBusinessObjectDataVersion the expected business object data version
     * @param expectedBusinessObjectDataAttributeName the expected business object data attribute name
     * @param expectedBusinessObjectDataAttributeValue the expected business object data attribute value
     * @param actualBusinessObjectDataAttribute the business object data attribute object instance to be validated
     */
    public void validateBusinessObjectDataAttribute(Integer businessObjectDataAttributeId, String expectedNamespace,
        String expectedBusinessObjectDefinitionName, String expectedBusinessObjectFormatUsage, String expectedBusinessObjectFormatFileType,
        Integer expectedBusinessObjectFormatVersion, String expectedBusinessObjectDataPartitionValue, List<String> expectedBusinessObjectDataSubPartitionValues,
        Integer expectedBusinessObjectDataVersion, String expectedBusinessObjectDataAttributeName, String expectedBusinessObjectDataAttributeValue,
        BusinessObjectDataAttribute actualBusinessObjectDataAttribute)
    {
        assertNotNull(actualBusinessObjectDataAttribute);
        if (businessObjectDataAttributeId != null)
        {
            assertEquals(businessObjectDataAttributeId, Integer.valueOf(actualBusinessObjectDataAttribute.getId()));
        }
        validateBusinessObjectDataAttributeKey(expectedNamespace, expectedBusinessObjectDefinitionName, expectedBusinessObjectFormatUsage,
            expectedBusinessObjectFormatFileType, expectedBusinessObjectFormatVersion, expectedBusinessObjectDataPartitionValue,
            expectedBusinessObjectDataSubPartitionValues, expectedBusinessObjectDataVersion, expectedBusinessObjectDataAttributeName,
            actualBusinessObjectDataAttribute.getBusinessObjectDataAttributeKey());
        assertEquals(expectedBusinessObjectDataAttributeValue, actualBusinessObjectDataAttribute.getBusinessObjectDataAttributeValue());
    }

    /**
     * Validates business object data attribute key against specified arguments.
     *
     * @param expectedNamespace the expected namespace
     * @param expectedBusinessObjectDefinitionName the expected business object definition name
     * @param expectedBusinessObjectFormatUsage the expected business object format usage
     * @param expectedBusinessObjectFormatFileType the expected business object format file type
     * @param expectedBusinessObjectFormatVersion the expected business object format version
     * @param expectedBusinessObjectDataPartitionValue the expected partition value
     * @param expectedBusinessObjectDataSubPartitionValues the expected subpartition values
     * @param expectedBusinessObjectDataVersion the expected business object data version
     * @param expectedBusinessObjectDataAttributeName the expected business object data attribute name
     * @param actualBusinessObjectDataAttributeKey the business object data attribute key object instance to be validated
     */
    public void validateBusinessObjectDataAttributeKey(String expectedNamespace, String expectedBusinessObjectDefinitionName,
        String expectedBusinessObjectFormatUsage, String expectedBusinessObjectFormatFileType, Integer expectedBusinessObjectFormatVersion,
        String expectedBusinessObjectDataPartitionValue, List<String> expectedBusinessObjectDataSubPartitionValues, Integer expectedBusinessObjectDataVersion,
        String expectedBusinessObjectDataAttributeName, BusinessObjectDataAttributeKey actualBusinessObjectDataAttributeKey)
    {
        assertNotNull(actualBusinessObjectDataAttributeKey);

        assertEquals(expectedNamespace, actualBusinessObjectDataAttributeKey.getNamespace());
        assertEquals(expectedBusinessObjectDefinitionName, actualBusinessObjectDataAttributeKey.getBusinessObjectDefinitionName());
        assertEquals(expectedBusinessObjectFormatUsage, actualBusinessObjectDataAttributeKey.getBusinessObjectFormatUsage());
        assertEquals(expectedBusinessObjectFormatFileType, actualBusinessObjectDataAttributeKey.getBusinessObjectFormatFileType());
        assertEquals(expectedBusinessObjectFormatVersion, actualBusinessObjectDataAttributeKey.getBusinessObjectFormatVersion());
        assertEquals(expectedBusinessObjectDataPartitionValue, actualBusinessObjectDataAttributeKey.getPartitionValue());
        assertEquals(expectedBusinessObjectDataSubPartitionValues, actualBusinessObjectDataAttributeKey.getSubPartitionValues());
        assertEquals(expectedBusinessObjectDataVersion, actualBusinessObjectDataAttributeKey.getBusinessObjectDataVersion());
        assertEquals(expectedBusinessObjectDataAttributeName, actualBusinessObjectDataAttributeKey.getBusinessObjectDataAttributeName());
    }
}
