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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.model.api.xml.AttributeValueListKey;
import org.finra.herd.model.api.xml.AttributeValueListKeys;
import org.finra.herd.model.jpa.AttributeValueListEntity;

public class AttributeValueListDaoTest extends AbstractDaoTest
{

    @Mock
    protected AttributeValueListDao attributeValueListDao;

    @Mock
    protected AttributeValueListDaoTestHelper attributeValueListDaoTestHelper;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testGetAttributeValueListByKey()
    {
        AttributeValueListEntity attributeValueListEntity = new AttributeValueListEntity();
        attributeValueListEntity.setNamespace(namespaceDao.getNamespaceByCd(NAMESPACE));
        attributeValueListEntity.setAttributeValueListName(ATTRIBUTE_VALUE_LIST_NAME);

        when(attributeValueListDao.getAttributeValueListByKey(new AttributeValueListKey(ATTRIBUTE_VALUE_LIST_NAMESPACE, ATTRIBUTE_VALUE_LIST_NAME)))
            .thenReturn(attributeValueListEntity);

        // Retrieve attribute value list entity.
        AttributeValueListEntity attributeValueListEntityResult =
            attributeValueListDao.getAttributeValueListByKey(new AttributeValueListKey(ATTRIBUTE_VALUE_LIST_NAMESPACE, ATTRIBUTE_VALUE_LIST_NAME));

        // Validate the results.
        assertNotNull(attributeValueListEntityResult);
        assertTrue(attributeValueListEntityResult.getAttributeValueListName().equals(ATTRIBUTE_VALUE_LIST_NAME));
    }


    @Test
    public void testGetAttributeValueListByKeyInvalidKey()
    {
        //return nul entity
        when(attributeValueListDao.getAttributeValueListByKey(new AttributeValueListKey(ATTRIBUTE_VALUE_LIST_NAMESPACE, "I DON EXIST"))).thenReturn(null);

        // Try to retrieve attribute value list entity using an invalid name.
        assertNull(attributeValueListDao.getAttributeValueListByKey(new AttributeValueListKey(ATTRIBUTE_VALUE_LIST_NAMESPACE, "I DON EXIST")));
    }

    @Test
    public void testGetAttributeValueListKeys()
    {
        /*
         * Get the namespaces which the current user is authorized to READ.
         * If a specific namespace was requested, and the current user is authorized to read the namespace, include ONLY the requested namespace.
         * If a specific namespace was requested, but the current user is not authorized to read the namespace, clear all namespaces.
         * Otherwise, include all authorized namespaces.
         *
         * This ensures that only authorized namespaces are queried from the database and that
         * an unauthorized user cannot determine if he specified an existing namespace or not.
         */
        List<String> authorizedNamespaces = new ArrayList<>();
        authorizedNamespaces.add("atrbt_value_list_test_namespace");

        AttributeValueListKeys attributeValueListKeys = new AttributeValueListKeys(getTestAttributeValueListKeys());

        //return nul entity
        when(attributeValueListDao.getAttributeValueListKeys(authorizedNamespaces)).thenReturn(attributeValueListKeys);

        // Get the list of attribute value lists.
        AttributeValueListKeys attributeValueListKeysResult = attributeValueListDao.getAttributeValueListKeys(authorizedNamespaces);

        // Validate the results.
        assertNotNull(attributeValueListKeysResult);
        assertTrue(attributeValueListKeysResult.getAttributeValueListKeys().containsAll(getTestAttributeValueListKeys()));
    }

    /**
     * Returns a list of test attribute value list keys.
     *
     * @return the list of test attribute value list keys
     */
    private List<AttributeValueListKey> getTestAttributeValueListKeys()
    {
        // Get a list of test file type keys.
        return Arrays.asList(new AttributeValueListKey(ATTRIBUTE_VALUE_LIST_NAMESPACE, ATTRIBUTE_VALUE_LIST_NAME),
            new AttributeValueListKey(ATTRIBUTE_VALUE_LIST_NAMESPACE, ATTRIBUTE_VALUE_LIST_NAME + "_DUPLICATE"));
    }
}
