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
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.dao.AttributeValueListDao;
import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.api.xml.AttributeValueList;
import org.finra.herd.model.api.xml.AttributeValueListCreateRequest;
import org.finra.herd.model.api.xml.AttributeValueListKey;
import org.finra.herd.model.api.xml.AttributeValueListKeys;
import org.finra.herd.model.api.xml.NamespacePermissionEnum;
import org.finra.herd.model.jpa.AttributeValueListEntity;
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.service.helper.AttributeValueListDaoHelper;
import org.finra.herd.service.helper.AttributeValueListHelper;
import org.finra.herd.service.helper.NamespaceDaoHelper;
import org.finra.herd.service.helper.NamespaceSecurityHelper;
import org.finra.herd.service.impl.AttributeValueListServiceImpl;

/**
 * This class tests the functionality of attribute value list service.
 */
public class AttributeValueListServiceTest extends AbstractServiceTest
{
    @Mock
    private AttributeValueListDao attributeValueListDao;

    @Mock
    private AttributeValueListDaoHelper attributeValueListDaoHelper;

    @Mock
    private AttributeValueListHelper attributeValueListHelper;

    @InjectMocks
    private AttributeValueListServiceImpl attributeValueListService;

    @Mock
    private NamespaceDaoHelper namespaceDaoHelper;

    @Mock
    private NamespaceSecurityHelper namespaceSecurityHelper;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void createAttributeValueList()
    {
        // Create an attribute value list key.
        AttributeValueListKey attributeValueListKey = new AttributeValueListKey(ATTRIBUTE_VALUE_LIST_NAMESPACE, ATTRIBUTE_VALUE_LIST_NAME);

        // Create an attribute value list create request.
        AttributeValueListCreateRequest request = new AttributeValueListCreateRequest(attributeValueListKey);

        // Create a namespace entity.
        NamespaceEntity namespaceEntity = new NamespaceEntity();
        namespaceEntity.setCode(ATTRIBUTE_VALUE_LIST_NAMESPACE);

        // Create an attribute value list entity.
        AttributeValueListEntity attributeValueListEntity = new AttributeValueListEntity();
        attributeValueListEntity.setId(ATTRIBUTE_VALUE_LIST_ID);
        attributeValueListEntity.setNamespace(namespaceEntity);
        attributeValueListEntity.setAttributeValueListName(ATTRIBUTE_VALUE_LIST_NAME);

        // Mock calls to external methods.
        when(namespaceDaoHelper.getNamespaceEntity(ATTRIBUTE_VALUE_LIST_NAMESPACE)).thenReturn(namespaceEntity);
        when(attributeValueListDao.getAttributeValueListByKey(attributeValueListKey)).thenReturn(null);
        when(attributeValueListDao.saveAndRefresh(any(AttributeValueListEntity.class))).thenReturn(attributeValueListEntity);

        // Call the method under test.
        AttributeValueList result = attributeValueListService.createAttributeValueList(request);

        // Verify the external calls.
        verify(attributeValueListHelper).validateAttributeValueListCreateRequest(request);
        verify(namespaceDaoHelper).getNamespaceEntity(ATTRIBUTE_VALUE_LIST_NAMESPACE);
        verify(attributeValueListDao).getAttributeValueListByKey(attributeValueListKey);
        verify(attributeValueListDao).saveAndRefresh(any(AttributeValueListEntity.class));
        verifyNoMoreInteractionsHelper();

        // Validate the result.
        assertEquals(new AttributeValueList(ATTRIBUTE_VALUE_LIST_ID, attributeValueListKey), result);
    }

    @Test
    public void createAttributeValueListAlreadyExists()
    {
        // Create an attribute value list key.
        AttributeValueListKey attributeValueListKey = new AttributeValueListKey(ATTRIBUTE_VALUE_LIST_NAMESPACE, ATTRIBUTE_VALUE_LIST_NAME);

        // Create an attribute value list create request.
        AttributeValueListCreateRequest request = new AttributeValueListCreateRequest(attributeValueListKey);

        // Mock calls to external methods.
        when(namespaceDaoHelper.getNamespaceEntity(ATTRIBUTE_VALUE_LIST_NAMESPACE)).thenReturn(new NamespaceEntity());
        when(attributeValueListDao.getAttributeValueListByKey(attributeValueListKey)).thenReturn(new AttributeValueListEntity());

        // Try to call the method under test.
        try
        {
            attributeValueListService.createAttributeValueList(request);
            fail();
        }
        catch (AlreadyExistsException e)
        {
            assertEquals(String
                .format("Unable to create attribute value list with name \"%s\" because it already exists for namespace \"%s\".", ATTRIBUTE_VALUE_LIST_NAME,
                    ATTRIBUTE_VALUE_LIST_NAMESPACE), e.getMessage());
        }

        // Verify the external calls.
        verify(attributeValueListHelper).validateAttributeValueListCreateRequest(request);
        verify(namespaceDaoHelper).getNamespaceEntity(ATTRIBUTE_VALUE_LIST_NAMESPACE);
        verify(attributeValueListDao).getAttributeValueListByKey(attributeValueListKey);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testDeleteAttributeValueList()
    {
        // Create an attribute value list key.
        AttributeValueListKey attributeValueListKey = new AttributeValueListKey(ATTRIBUTE_VALUE_LIST_NAMESPACE, ATTRIBUTE_VALUE_LIST_NAME);

        // Create a namespace entity.
        NamespaceEntity namespaceEntity = new NamespaceEntity();
        namespaceEntity.setCode(ATTRIBUTE_VALUE_LIST_NAMESPACE);

        // Create an attribute value list entity.
        AttributeValueListEntity attributeValueListEntity = new AttributeValueListEntity();
        attributeValueListEntity.setId(ATTRIBUTE_VALUE_LIST_ID);
        attributeValueListEntity.setNamespace(namespaceEntity);
        attributeValueListEntity.setAttributeValueListName(ATTRIBUTE_VALUE_LIST_NAME);

        // Mock calls to external methods.
        when(attributeValueListDaoHelper.getAttributeValueListEntity(attributeValueListKey)).thenReturn(attributeValueListEntity);

        // Call the method under test.
        AttributeValueList result = attributeValueListService.deleteAttributeValueList(attributeValueListKey);

        // Verify the external calls.
        verify(attributeValueListHelper).validateAttributeValueListKey(attributeValueListKey);
        verify(attributeValueListDaoHelper).getAttributeValueListEntity(attributeValueListKey);
        verify(attributeValueListDao).delete(attributeValueListEntity);
        verifyNoMoreInteractionsHelper();

        // Validate the result.
        assertEquals(new AttributeValueList(ATTRIBUTE_VALUE_LIST_ID, attributeValueListKey), result);
    }

    @Test
    public void testGetAttributeValueList()
    {
        // Create an attribute value list key.
        AttributeValueListKey attributeValueListKey = new AttributeValueListKey(ATTRIBUTE_VALUE_LIST_NAMESPACE, ATTRIBUTE_VALUE_LIST_NAME);

        // Create a namespace entity.
        NamespaceEntity namespaceEntity = new NamespaceEntity();
        namespaceEntity.setCode(ATTRIBUTE_VALUE_LIST_NAMESPACE);

        // Create an attribute value list entity.
        AttributeValueListEntity attributeValueListEntity = new AttributeValueListEntity();
        attributeValueListEntity.setId(ATTRIBUTE_VALUE_LIST_ID);
        attributeValueListEntity.setNamespace(namespaceEntity);
        attributeValueListEntity.setAttributeValueListName(ATTRIBUTE_VALUE_LIST_NAME);

        // Mock calls to external methods.
        when(attributeValueListDaoHelper.getAttributeValueListEntity(attributeValueListKey)).thenReturn(attributeValueListEntity);

        // Call the method under test.
        AttributeValueList result = attributeValueListService.getAttributeValueList(attributeValueListKey);

        // Verify the external calls.
        verify(attributeValueListHelper).validateAttributeValueListKey(attributeValueListKey);
        verify(attributeValueListDaoHelper).getAttributeValueListEntity(attributeValueListKey);
        verifyNoMoreInteractionsHelper();

        // Validate the result.
        assertEquals(new AttributeValueList(ATTRIBUTE_VALUE_LIST_ID, attributeValueListKey), result);
    }

    @Test
    public void testGetAttributeValueLists()
    {
        // Create a set of authorized namespaces.
        Set<String> authorizedNamespaces = new HashSet<>();
        authorizedNamespaces.add(NAMESPACE);

        // Create an attribute value list key.
        List<AttributeValueListKey> attributeValueListKeys =
            Arrays.asList(new AttributeValueListKey(ATTRIBUTE_VALUE_LIST_NAMESPACE, ATTRIBUTE_VALUE_LIST_NAME));

        // Mock calls to external methods.
        when(namespaceSecurityHelper.getAuthorizedNamespaces(NamespacePermissionEnum.READ)).thenReturn(authorizedNamespaces);
        when(attributeValueListDao.getAttributeValueLists(authorizedNamespaces)).thenReturn(attributeValueListKeys);

        // Call the method under test.
        AttributeValueListKeys result = attributeValueListService.getAttributeValueLists();

        // Verify the external calls.
        verify(namespaceSecurityHelper).getAuthorizedNamespaces(NamespacePermissionEnum.READ);
        verify(attributeValueListDao).getAttributeValueLists(authorizedNamespaces);
        verifyNoMoreInteractionsHelper();

        // Validate the result.
        assertEquals(new AttributeValueListKeys(attributeValueListKeys), result);
    }

    @Test
    public void testGetAttributeValueListsNoAuthorizedNamespaces()
    {
        // Create an empty set of authorized namespaces.
        Set<String> authorizedNamespaces = new HashSet<>();

        // Mock calls to external methods.
        when(namespaceSecurityHelper.getAuthorizedNamespaces(NamespacePermissionEnum.READ)).thenReturn(authorizedNamespaces);

        // Call the method under test.
        AttributeValueListKeys result = attributeValueListService.getAttributeValueLists();

        // Verify the external calls.
        verify(namespaceSecurityHelper).getAuthorizedNamespaces(NamespacePermissionEnum.READ);
        verifyNoMoreInteractionsHelper();

        // Validate the result.
        assertEquals(new AttributeValueListKeys(), result);
    }

    /**
     * Checks if any of the mocks has any interaction.
     */
    private void verifyNoMoreInteractionsHelper()
    {
        verifyNoMoreInteractions(attributeValueListDao, attributeValueListDaoHelper, attributeValueListHelper, namespaceDaoHelper, namespaceSecurityHelper);
    }
}
