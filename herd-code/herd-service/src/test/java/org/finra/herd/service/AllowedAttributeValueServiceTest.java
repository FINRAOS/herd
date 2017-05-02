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

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.dao.AllowedAttributeValueDao;
import org.finra.herd.service.helper.AlternateKeyHelper;
import org.finra.herd.service.helper.AttributeValueListHelper;
import org.finra.herd.service.impl.AllowedAttributeValueServiceImpl;

/**
 * This class tests the functionality of allowed attribute values service.
 */
public class AllowedAttributeValueServiceTest extends AbstractServiceTest
{
    @InjectMocks
    private AllowedAttributeValueServiceImpl allowedAttributeValueService;

    @Mock
    private AlternateKeyHelper alternateKeyHelper;

    @Mock
    private AllowedAttributeValueDao allowedAttributeValueDao;

    @Mock
    private AttributeValueListHelper attributeValueListHelper;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCreateAllowedAttributeValues(){

//        // Create attribute value list key
//        AttributeValueListKey attributeValueListKey = new AttributeValueListKey(NAMESPACE_CODE, ATTRIBUTE_VALUE_LIST_NAME);
//
//        // Create namespace entity
//        NamespaceEntity namespaceEntity = new NamespaceEntity();
//        namespaceEntity.setCode(NAMESPACE_CODE);
//
//        // Create attribute value list entity
//        AttributeValueListEntity attributeValueListEntity = new AttributeValueListEntity();
//        attributeValueListEntity.setId(ATTRIBUTE_VALUE_LIST_ID);
//        attributeValueListEntity.setNamespace(namespaceEntity);
//        attributeValueListEntity.setAttributeValueListName(ATTRIBUTE_VALUE_LIST_NAME);
//
//        // Create allowed attribute value entity
//        AllowedAttributeValueEntity allowedAttributeValueEntity = new AllowedAttributeValueEntity();
//        allowedAttributeValueEntity.setAllowedAttributeValue(allowedAttributeValueDaoTestHelper.getTestSortedAllowedAttributeValues().get(0));
//        allowedAttributeValueEntity.setAttributeValueListEntity(attributeValueListEntity);
//
//        // Create the response
//        AllowedAttributeValuesInformation allowedAttributeValuesInformation =  new AllowedAttributeValuesInformation();
//        allowedAttributeValuesInformation.setAllowedAttributeValues(allowedAttributeValueDaoTestHelper.getTestSortedAllowedAttributeValues());
//        allowedAttributeValuesInformation.setAttributeValueListKey(attributeValueListKey);
//
//        // Mock calls to external method
//        when(attributeValueListHelper.getAttributeValueListEntity(attributeValueListKey)).thenReturn(attributeValueListEntity);
//        when(allowedAttributeValueDao.saveAndRefresh(any(AllowedAttributeValueEntity.class))).thenReturn(allowedAttributeValueEntity);
//
//        // Call method under test
//        AllowedAttributeValuesInformation response = allowedAttributeValueService.createAllowedAttributeValues(new AllowedAttributeValuesCreateRequest(attributeValueListKey, allowedAttributeValueDaoTestHelper.getTestSortedAllowedAttributeValues()));
//
//        // Validate the response
//        assertEquals(allowedAttributeValuesInformation, response);
    }
}

