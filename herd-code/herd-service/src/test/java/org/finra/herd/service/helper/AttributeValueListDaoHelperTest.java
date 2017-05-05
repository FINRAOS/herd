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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.dao.AttributeValueListDao;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.AttributeValueListKey;
import org.finra.herd.model.jpa.AttributeValueListEntity;
import org.finra.herd.service.AbstractServiceTest;

public class AttributeValueListDaoHelperTest extends AbstractServiceTest
{
    @Mock
    private AttributeValueListDao attributeValueListDao;

    @InjectMocks
    private AttributeValueListDaoHelper attributeValueListDaoHelper;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testGetAttributeValueListEntity()
    {
        // Create an attribute value list key.
        AttributeValueListKey attributeValueListKey = new AttributeValueListKey(ATTRIBUTE_VALUE_LIST_NAMESPACE, ATTRIBUTE_VALUE_LIST_NAME);

        // Create an attribute value list entity.
        AttributeValueListEntity attributeValueListEntity = new AttributeValueListEntity();

        // Mock calls to external methods.
        when(attributeValueListDao.getAttributeValueListByKey(attributeValueListKey)).thenReturn(attributeValueListEntity);

        // Call the method under test.
        AttributeValueListEntity result = attributeValueListDaoHelper.getAttributeValueListEntity(attributeValueListKey);

        // Verify the external calls.
        verify(attributeValueListDao).getAttributeValueListByKey(attributeValueListKey);
        verifyNoMoreInteractions(attributeValueListDao);

        // Validate the result.
        assertEquals(attributeValueListEntity, result);
    }

    @Test
    public void testGetAttributeValueListEntityAttributeValueListEntityNoExists()
    {
        // Create an attribute value list key.
        AttributeValueListKey attributeValueListKey = new AttributeValueListKey(ATTRIBUTE_VALUE_LIST_NAMESPACE, ATTRIBUTE_VALUE_LIST_NAME);

        // Mock calls to external methods.
        when(attributeValueListDao.getAttributeValueListByKey(attributeValueListKey)).thenReturn(null);

        // Try to call the method under test.
        try
        {
            attributeValueListDaoHelper.getAttributeValueListEntity(attributeValueListKey);
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String
                .format("Attribute value list with name \"%s\" doesn't exist for namespace \"%s\".", ATTRIBUTE_VALUE_LIST_NAME, ATTRIBUTE_VALUE_LIST_NAMESPACE),
                e.getMessage());
        }

        // Verify the external calls.
        verify(attributeValueListDao).getAttributeValueListByKey(attributeValueListKey);
        verifyNoMoreInteractions(attributeValueListDao);
    }
}
