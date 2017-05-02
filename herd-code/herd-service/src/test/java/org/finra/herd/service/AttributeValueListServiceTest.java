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

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.dao.AttributeValueListDao;
import org.finra.herd.dao.NamespaceDao;
import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.AttributeValueList;
import org.finra.herd.model.api.xml.AttributeValueListCreateRequest;
import org.finra.herd.model.api.xml.AttributeValueListKey;
import org.finra.herd.model.api.xml.AttributeValueListKeys;
import org.finra.herd.model.api.xml.Namespace;
import org.finra.herd.model.api.xml.NamespaceKey;
import org.finra.herd.model.jpa.AttributeValueListEntity;
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.service.helper.AttributeValueListHelper;
import org.finra.herd.service.helper.NamespaceDaoHelper;
import org.finra.herd.service.impl.AttributeValueListServiceImpl;

/**
 * IndexSearchServiceTest
 */
public class AttributeValueListServiceTest extends AbstractServiceTest
{
    private static final int ATTRIBUTE_VALUE_LIST_ID = 1009;

    private static final String NAMESPACE = "test_attribute_value_namespace";

    private static final int ONE_TIME = 1;

    private static final int TWO_TIMES = 2;

    private static final String attribute_value_list_name = "test_attribute_value_list_name";

    @Mock
    private AttributeValueListDao attributeValueListDao;

    @Mock
    private AttributeValueListHelper attributeValueListHelper;

    @InjectMocks
    private AttributeValueListServiceImpl attributeValueListService;

    @Mock
    private NamespaceDao namespaceDao;

    @Mock
    private NamespaceDaoHelper namespaceDaoHelper;

    @Mock
    private NamespaceService namespaceService;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    public void createAttributeValueList(String namespaceCd, String attribute_value_list_name)
    {
        //namespace object for testing
        NamespaceEntity namespaceEntity = new NamespaceEntity();
        namespaceEntity.setCode(namespaceCd);
        NamespaceKey namespaceKey = new NamespaceKey(namespaceCd);
        Namespace namespace = new Namespace(namespaceCd);

        // Create attribute value list request
        AttributeValueListKey attributeValueListKey = new AttributeValueListKey(namespaceCd, attribute_value_list_name);
        AttributeValueListCreateRequest attributeValueListCreateRequest = new AttributeValueListCreateRequest(attributeValueListKey);
        AttributeValueListEntity attributeValueListEntity = new AttributeValueListEntity();
        attributeValueListEntity.setId(ATTRIBUTE_VALUE_LIST_ID);
        attributeValueListEntity.setNamespace(namespaceEntity);
        attributeValueListEntity.setAttributeValueListName(attribute_value_list_name);
        AttributeValueList attributeValueList = new AttributeValueList(ATTRIBUTE_VALUE_LIST_ID, attributeValueListKey);

        //Mock the call to namespace service
        when(namespaceDao.getNamespaceByCd(namespaceCd)).thenReturn(namespaceEntity);
        when(namespaceDaoHelper.getNamespaceEntity(namespaceCd)).thenReturn(namespaceEntity);
        when(namespaceService.getNamespace(namespaceKey)).thenReturn(namespace);

        // Mock the call to the attribute value list service
        when(attributeValueListHelper.getAttributeValueListEntity(attributeValueListCreateRequest.getAttributeValueListKey()))
            .thenReturn(attributeValueListEntity);
        when(attributeValueListDao.saveAndRefresh(attributeValueListEntity)).thenReturn(attributeValueListEntity);

        // Call the method under test
        AttributeValueList attributeValueListResult = attributeValueListService.createAttributeValueList(attributeValueListCreateRequest);

        // Verify the method call to indexSearchService.indexSearch()
        verify(attributeValueListDao, times(ONE_TIME)).saveAndRefresh(attributeValueListEntity);


        //validate the attribute value list key is as expected
        assertEquals(attributeValueListResult.getAttributeValueListKey(), new AttributeValueListKey(namespaceCd, attribute_value_list_name));

        // Validate the returned object.
        assertThat("Attribute value list response was null.", attributeValueListResult, not(nullValue()));
        assertThat("Attribute value list response was not an instance of IndexSearchResponse.class.", attributeValueListResult,
            instanceOf(AttributeValueList.class));
    }

    @Test
    public void testAttributeValueListDelete()
    {
        // Create attribute value list request
        NamespaceEntity namespaceEntity = new NamespaceEntity();
        namespaceEntity.setCode(null);
        AttributeValueListKey attributeValueListKey = new AttributeValueListKey(NAMESPACE, attribute_value_list_name);
        AttributeValueListEntity attributeValueListEntity = new AttributeValueListEntity();
        attributeValueListEntity.setId(ATTRIBUTE_VALUE_LIST_ID);
        attributeValueListEntity.setNamespace(namespaceEntity);
        attributeValueListEntity.setAttributeValueListName(attribute_value_list_name);

        when(attributeValueListHelper.getAttributeValueListEntity(attributeValueListKey)).thenReturn(attributeValueListEntity);

        // Call the method under test
        AttributeValueListKey attributeValueListKeyResult = attributeValueListService.deleteAttributeValueList(attributeValueListKey);

        // Verify the method call to indexSearchService.indexSearch()
        verify(attributeValueListDao, times(ONE_TIME)).delete(attributeValueListEntity);
        verifyNoMoreInteractions(attributeValueListDao);

        //validate the attribute value list key is as expected
        assertEquals(attributeValueListKeyResult,
            new AttributeValueListKey(attributeValueListEntity.getNamespace().getCode(), attributeValueListEntity.getAttributeValueListName()));

        // Validate the returned object.
        assertThat("Attribute value list response was null.", attributeValueListKeyResult, not(nullValue()));
        assertThat("Attribute value list response was not an instance of IndexSearchResponse.class.", attributeValueListKeyResult,
            instanceOf(AttributeValueListKey.class));
    }

    @Test(expected = AlreadyExistsException.class)
    public void testCreateAttributeValueListDuplicateNamespace()
    {
        //namespace object for testing
        NamespaceEntity namespaceEntity = new NamespaceEntity();
        namespaceEntity.setCode(null);
        NamespaceKey namespaceKey = new NamespaceKey(NAMESPACE);
        Namespace namespace = new Namespace(NAMESPACE);

        // Create attribute value list request
        AttributeValueListKey attributeValueListKey = new AttributeValueListKey(NAMESPACE, attribute_value_list_name);
        AttributeValueListCreateRequest attributeValueListCreateRequest = new AttributeValueListCreateRequest(attributeValueListKey);
        AttributeValueListEntity attributeValueListEntity = new AttributeValueListEntity();
        attributeValueListEntity.setId(ATTRIBUTE_VALUE_LIST_ID);
        attributeValueListEntity.setNamespace(namespaceEntity);
        attributeValueListEntity.setAttributeValueListName(attribute_value_list_name);

        //Mock the call to namespace service
        when(namespaceDao.getNamespaceByCd(NAMESPACE)).thenReturn(namespaceEntity);
        when(namespaceDaoHelper.getNamespaceEntity(null)).thenReturn(namespaceEntity);
        when(namespaceService.getNamespace(namespaceKey)).thenReturn(namespace);

        // Mock the call to the attribute value list service
        when(attributeValueListHelper.getAttributeValueListEntity(attributeValueListCreateRequest.getAttributeValueListKey()))
            .thenReturn(attributeValueListEntity);
        when(attributeValueListDao.getAttributeValueListByKey(attributeValueListKey)).thenReturn(attributeValueListEntity);

        // Call the method under test
        attributeValueListService.createAttributeValueList(attributeValueListCreateRequest);

    }

    @Test
    public void testCreateAttributeValueListHappyPath()
    {
        createAttributeValueList(NAMESPACE, ATTRIBUTE_VALUE_LIST_NAME);
    }

    @Test(expected = ObjectNotFoundException.class)
    public void testCreateAttributeValueListWithoutNamespace()
    {
        //namespace object for testing
        NamespaceEntity namespaceEntity = new NamespaceEntity();
        namespaceEntity.setCode(null);
        NamespaceKey namespaceKey = new NamespaceKey(null);
        Namespace namespace = new Namespace(null);

        // Create attribute value list request
        AttributeValueListKey attributeValueListKey = new AttributeValueListKey(null, attribute_value_list_name);
        AttributeValueListCreateRequest attributeValueListCreateRequest = new AttributeValueListCreateRequest(attributeValueListKey);
        AttributeValueListEntity attributeValueListEntity = new AttributeValueListEntity();
        attributeValueListEntity.setId(ATTRIBUTE_VALUE_LIST_ID);
        attributeValueListEntity.setNamespace(namespaceEntity);
        attributeValueListEntity.setAttributeValueListName(attribute_value_list_name);
        AttributeValueList attributeValueList = new AttributeValueList(ATTRIBUTE_VALUE_LIST_ID, attributeValueListKey);

        //Mock the call to namespace service
        when(namespaceDao.getNamespaceByCd(null)).thenReturn(null);
        when(namespaceDaoHelper.getNamespaceEntity(null)).thenReturn(null);
        when(namespaceService.getNamespace(namespaceKey)).thenReturn(null);

        // Mock the call to the attribute value list service
        when(attributeValueListHelper.getAttributeValueListEntity(attributeValueListCreateRequest.getAttributeValueListKey()))
            .thenReturn(attributeValueListEntity);
        when(attributeValueListDao.saveAndRefresh(attributeValueListEntity)).thenReturn(attributeValueListEntity);
        //when(attributeValueListService.createAttributeValueList(attributeValueListCreateRequest)).thenReturn(attributeValueList);

        // Call the method under test
        attributeValueListService.createAttributeValueList(attributeValueListCreateRequest);

    }

    @Test
    public void testGetAttributeValueList()
    {
        // Create attribute value list request
        NamespaceEntity namespaceEntity = new NamespaceEntity();
        namespaceEntity.setCode(NAMESPACE);

        AttributeValueListKey attributeValueListKey = new AttributeValueListKey(NAMESPACE, attribute_value_list_name);
        AttributeValueListEntity attributeValueListEntity = new AttributeValueListEntity();
        attributeValueListEntity.setId(ATTRIBUTE_VALUE_LIST_ID);
        attributeValueListEntity.setNamespace(namespaceEntity);
        attributeValueListEntity.setAttributeValueListName(attribute_value_list_name);

        // Mock the call to the attribute value list service
        when(attributeValueListDao.getAttributeValueListByKey(attributeValueListKey)).thenReturn(attributeValueListEntity);

        // Call the method under test
        AttributeValueList attributeValueListResult = attributeValueListService.getAttributeValueList(attributeValueListKey);

        // Verify the method call to indexSearchService.indexSearch()
        verify(attributeValueListDao, times(ONE_TIME)).getAttributeValueListByKey(attributeValueListKey);
        verifyNoMoreInteractions(attributeValueListDao);

        //validate the attribute value list key is as expected
        assertEquals(attributeValueListResult.getAttributeValueListKey(), attributeValueListKey);

        // Validate the returned object.
        assertThat("Attribute value list response was null.", attributeValueListResult, not(nullValue()));
        assertThat("Attribute value list response was not an instance of IndexSearchResponse.class.", attributeValueListResult,
            instanceOf(AttributeValueList.class));
    }

    @Test
    public void testGetAttributeValueListKeys()
    {
        // Create attribute value list request
        AttributeValueListKey attributeValueListKey = new AttributeValueListKey(NAMESPACE, attribute_value_list_name);
        AttributeValueListKey attributeValueListKeyDuplicate = new AttributeValueListKey(NAMESPACE, attribute_value_list_name);
        AttributeValueListKeys attributeValueListKeys = new AttributeValueListKeys(Arrays.asList(attributeValueListKey, attributeValueListKeyDuplicate));

        // Mock the call to the attribute value list service
        when(attributeValueListDao.getAttributeValueListKeys()).thenReturn(attributeValueListKeys);

        // Call the method under test
        AttributeValueListKeys attributeValueListKeysResult = attributeValueListService.getAttributeValueListKeys();

        // Verify the method call to indexSearchService.indexSearch()
        verify(attributeValueListDao, times(ONE_TIME)).getAttributeValueListKeys();
        verifyNoMoreInteractions(attributeValueListDao);

        //validate the attribute value list key is as expected
        assertEquals(attributeValueListKeysResult, attributeValueListKeys);

        // Validate the returned object.
        assertThat("Attribute value list response was null.", attributeValueListKeysResult, not(nullValue()));
        assertThat("Attribute value list response was not an instance of IndexSearchResponse.class.", attributeValueListKeysResult,
            instanceOf(AttributeValueListKeys.class));
    }

}
