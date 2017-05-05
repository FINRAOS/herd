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

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import org.finra.herd.model.api.xml.AttributeValueListKey;
import org.finra.herd.model.jpa.AllowedAttributeValueEntity;


public class AllowedAttributeValueDaoTest extends AbstractDaoTest
{
    @Autowired
    private AllowedAttributeValueDao allowedAttributeValueDao;

    @Test
    public void testGetAllowedAttributeValues()
    {
        // Create attribute value list key.
        AttributeValueListKey attributeValueListKey = new AttributeValueListKey(NAMESPACE_CODE, ATTRIBUTE_VALUE_LIST_NAME);
        List<String> allowedAttributeValueList = allowedAttributeValueDaoTestHelper.getTestUnsortedAllowedAttributeValues();

        // Create and persist a attribute value list key entity.
        attributeValueListDaoTestHelper.createAttributeValueListEntity(NAMESPACE_CODE, ATTRIBUTE_VALUE_LIST_NAME);

        // Create and persist a list of allowed attribute values.
        allowedAttributeValueDaoTestHelper.createAllowedAttributeValueEntities(attributeValueListKey, allowedAttributeValueList);

        // Get the allowed attribute values for the specified key.
        List<AllowedAttributeValueEntity> responseEntities = allowedAttributeValueDao.getAllowedAttributeValuesByAttributeValueListKey(attributeValueListKey);

        // Create a list of allowed attribute values.
        List<String> allowedAttributesResponse = new ArrayList<>();
        responseEntities.forEach((responseEntity) -> {
            allowedAttributesResponse.add(responseEntity.getAllowedAttributeValue());
        });

        // Validate the response is sorted by allowed attribute values.
        Collections.sort(allowedAttributeValueList);
        assertEquals(allowedAttributeValueList, allowedAttributesResponse);
    }
}
