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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.dao.AllowedAttributeValueDao;
import org.finra.herd.model.api.xml.AllowedAttributeValuesCreateRequest;
import org.finra.herd.model.api.xml.AllowedAttributeValuesDeleteRequest;
import org.finra.herd.model.api.xml.AllowedAttributeValuesInformation;
import org.finra.herd.model.api.xml.AttributeValueListKey;
import org.finra.herd.model.jpa.AllowedAttributeValueEntity;
import org.finra.herd.model.jpa.AttributeValueListEntity;
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.service.helper.AlternateKeyHelper;
import org.finra.herd.service.helper.AttributeValueListDaoHelper;
import org.finra.herd.service.helper.AttributeValueListHelper;
import org.finra.herd.service.impl.AllowedAttributeValueServiceImpl;

/**
 * This class tests the functionality of allowed attribute values service.
 */
public class AllowedAttributeValueServiceTest extends AbstractServiceTest
{
    @Mock
    private AllowedAttributeValueDao allowedAttributeValueDao;

    @InjectMocks
    private AllowedAttributeValueServiceImpl allowedAttributeValueService;

    @Mock
    private AlternateKeyHelper alternateKeyHelper;

    @Mock
    private AttributeValueListDaoHelper attributeValueListDaoHelper;

    @Mock
    private AttributeValueListHelper attributeValueListHelper;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCreateAllowedAttributeValues()
    {

        // Create attribute value list key.
        AttributeValueListKey attributeValueListKey = new AttributeValueListKey(NAMESPACE_CODE, ATTRIBUTE_VALUE_LIST_NAME);

        // Create namespace entity.
        NamespaceEntity namespaceEntity = new NamespaceEntity();
        namespaceEntity.setCode(NAMESPACE_CODE);

        // Create attribute value list entity.
        AttributeValueListEntity attributeValueListEntity = new AttributeValueListEntity();
        attributeValueListEntity.setId(ATTRIBUTE_VALUE_LIST_ID);
        attributeValueListEntity.setNamespace(namespaceEntity);
        attributeValueListEntity.setAttributeValueListName(ATTRIBUTE_VALUE_LIST_NAME);
        attributeValueListEntity.setAllowedAttributeValues(new ArrayList<>());

        // Create allowed attribute value entity.
        AllowedAttributeValueEntity allowedAttributeValueEntity = new AllowedAttributeValueEntity();
        allowedAttributeValueEntity.setAllowedAttributeValue(ALLOWED_ATTRIBUTE_VALUE);
        allowedAttributeValueEntity.setAttributeValueListEntity(attributeValueListEntity);

        // Mock calls to external method.
        when(attributeValueListDaoHelper.getAttributeValueListEntity(attributeValueListKey)).thenReturn(attributeValueListEntity);
        when(allowedAttributeValueDao.saveAndRefresh(any(AllowedAttributeValueEntity.class))).thenReturn(allowedAttributeValueEntity);
        when(alternateKeyHelper.validateStringParameter("An", "allowed attribute value", ALLOWED_ATTRIBUTE_VALUE)).thenReturn(ALLOWED_ATTRIBUTE_VALUE);

        // Call method under test.
        AllowedAttributeValuesInformation response = allowedAttributeValueService
            .createAllowedAttributeValues(new AllowedAttributeValuesCreateRequest(attributeValueListKey, Arrays.asList(ALLOWED_ATTRIBUTE_VALUE)));

        // Verify the calls.
        verify(attributeValueListDaoHelper).getAttributeValueListEntity(attributeValueListKey);
        verify(allowedAttributeValueDao, times(2)).saveAndRefresh(any(AllowedAttributeValueEntity.class));
        verify(alternateKeyHelper).validateStringParameter("An", "allowed attribute value", ALLOWED_ATTRIBUTE_VALUE);

        // Validate the response.
        assertEquals(attributeValueListKey, response.getAttributeValueListKey());
        assertEquals(Arrays.asList(ALLOWED_ATTRIBUTE_VALUE), response.getAllowedAttributeValues());
    }

    @Test
    public void testCreateAllowedAttributeValuesAlreadyExists()
    {

        // Create attribute value list key.
        AttributeValueListKey attributeValueListKey = new AttributeValueListKey(NAMESPACE_CODE, ATTRIBUTE_VALUE_LIST_NAME);

        // Create namespace entity.
        NamespaceEntity namespaceEntity = new NamespaceEntity();
        namespaceEntity.setCode(NAMESPACE_CODE);

        // Create attribute value list entity.
        Collection<AllowedAttributeValueEntity> allowedAttributeValueEntities = new ArrayList<>();
        AttributeValueListEntity attributeValueListEntity = new AttributeValueListEntity();
        attributeValueListEntity.setId(ATTRIBUTE_VALUE_LIST_ID);
        attributeValueListEntity.setNamespace(namespaceEntity);
        attributeValueListEntity.setAttributeValueListName(ATTRIBUTE_VALUE_LIST_NAME);
        attributeValueListEntity.setAllowedAttributeValues(allowedAttributeValueEntities);

        // Create allowed attribute value entity.
        AllowedAttributeValueEntity allowedAttributeValueEntity = new AllowedAttributeValueEntity();
        allowedAttributeValueEntity.setAllowedAttributeValue(ALLOWED_ATTRIBUTE_VALUE);
        allowedAttributeValueEntity.setAttributeValueListEntity(attributeValueListEntity);
        allowedAttributeValueEntities.add(allowedAttributeValueEntity);

        // Mock calls to external method.
        when(attributeValueListDaoHelper.getAttributeValueListEntity(attributeValueListKey)).thenReturn(attributeValueListEntity);
        when(alternateKeyHelper.validateStringParameter("An", "allowed attribute value", ALLOWED_ATTRIBUTE_VALUE)).thenReturn(ALLOWED_ATTRIBUTE_VALUE);

        // Call method under test.
        try
        {
            allowedAttributeValueService
                .createAllowedAttributeValues(new AllowedAttributeValuesCreateRequest(attributeValueListKey, Arrays.asList(ALLOWED_ATTRIBUTE_VALUE)));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Allowed attribute value \"%s\" already exists in \"%s\" attribute value list.", ALLOWED_ATTRIBUTE_VALUE,
                attributeValueListEntity.getAttributeValueListName()), e.getMessage());
        }

        verify(attributeValueListDaoHelper).getAttributeValueListEntity(attributeValueListKey);
        verify(alternateKeyHelper).validateStringParameter("An", "allowed attribute value", ALLOWED_ATTRIBUTE_VALUE);
    }

    @Test
    public void testCreateAllowedAttributeValuesAlreadyMissingRequiredParams()
    {

        try
        {
            allowedAttributeValueService.createAllowedAttributeValues(null);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("An allowed attribute value create request must be specified.", e.getMessage());
        }

        // Create attribute value list key.
        AttributeValueListKey attributeValueListKey = new AttributeValueListKey(NAMESPACE_CODE, ATTRIBUTE_VALUE_LIST_NAME);
        AllowedAttributeValuesCreateRequest request = new AllowedAttributeValuesCreateRequest(attributeValueListKey, NO_ALLOWED_ATTRIBUTE_VALUES);

        try
        {
            allowedAttributeValueService.createAllowedAttributeValues(request);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("At least one allowed attribute value must be specified.", e.getMessage());
        }

        request = new AllowedAttributeValuesCreateRequest(attributeValueListKey, Arrays.asList(ALLOWED_ATTRIBUTE_VALUE, ALLOWED_ATTRIBUTE_VALUE));

        // Mock calls to external methods.
        when(alternateKeyHelper.validateStringParameter("An", "allowed attribute value", ALLOWED_ATTRIBUTE_VALUE)).thenReturn(ALLOWED_ATTRIBUTE_VALUE);

        try
        {
            allowedAttributeValueService.createAllowedAttributeValues(request);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Duplicate allowed attribute value \"%s\" found.", ALLOWED_ATTRIBUTE_VALUE), e.getMessage());
        }

        verify(alternateKeyHelper, times(2)).validateStringParameter("An", "allowed attribute value", ALLOWED_ATTRIBUTE_VALUE);
    }

    @Test
    public void testDeleteAllowedAttributeValues()
    {

        // Create attribute value list key.
        AttributeValueListKey attributeValueListKey = new AttributeValueListKey(NAMESPACE_CODE, ATTRIBUTE_VALUE_LIST_NAME);

        // Create namespace entity.
        NamespaceEntity namespaceEntity = new NamespaceEntity();
        namespaceEntity.setCode(NAMESPACE_CODE);

        // Create attribute value list entity.
        Collection<AllowedAttributeValueEntity> allowedAttributeValueEntities = new ArrayList<>();
        AttributeValueListEntity attributeValueListEntity = new AttributeValueListEntity();
        attributeValueListEntity.setId(ATTRIBUTE_VALUE_LIST_ID);
        attributeValueListEntity.setNamespace(namespaceEntity);
        attributeValueListEntity.setAttributeValueListName(ATTRIBUTE_VALUE_LIST_NAME);
        attributeValueListEntity.setAllowedAttributeValues(allowedAttributeValueEntities);

        // Create allowed attribute value entity.
        AllowedAttributeValueEntity allowedAttributeValueEntity = new AllowedAttributeValueEntity();
        allowedAttributeValueEntity.setAllowedAttributeValue(ALLOWED_ATTRIBUTE_VALUE);
        allowedAttributeValueEntity.setAttributeValueListEntity(attributeValueListEntity);
        allowedAttributeValueEntities.add(allowedAttributeValueEntity);

        // Mock calls to external method.
        when(attributeValueListDaoHelper.getAttributeValueListEntity(attributeValueListKey)).thenReturn(attributeValueListEntity);
        when(allowedAttributeValueDao.saveAndRefresh(any(AllowedAttributeValueEntity.class))).thenReturn(allowedAttributeValueEntity);
        when(alternateKeyHelper.validateStringParameter("An", "allowed attribute value", ALLOWED_ATTRIBUTE_VALUE)).thenReturn(ALLOWED_ATTRIBUTE_VALUE);

        // Call method under test.
        AllowedAttributeValuesInformation response = allowedAttributeValueService
            .deleteAllowedAttributeValues(new AllowedAttributeValuesDeleteRequest(attributeValueListKey, Arrays.asList(ALLOWED_ATTRIBUTE_VALUE)));

        // Verify the calls.
        verify(attributeValueListDaoHelper).getAttributeValueListEntity(attributeValueListKey);
        verify(allowedAttributeValueDao).saveAndRefresh(any(AllowedAttributeValueEntity.class));
        verify(alternateKeyHelper).validateStringParameter("An", "allowed attribute value", ALLOWED_ATTRIBUTE_VALUE);

        // Validate the response.
        assertEquals(attributeValueListKey, response.getAttributeValueListKey());
        assertEquals(Arrays.asList(ALLOWED_ATTRIBUTE_VALUE), response.getAllowedAttributeValues());
    }

    @Test
    public void testDeleteAllowedAttributeValuesAlreadyMissingRequiredParams()
    {

        try
        {
            allowedAttributeValueService.deleteAllowedAttributeValues(null);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("An allowed attribute value delete request must be specified.", e.getMessage());
        }

        // Create attribute value list key.
        AttributeValueListKey attributeValueListKey = new AttributeValueListKey(NAMESPACE_CODE, ATTRIBUTE_VALUE_LIST_NAME);
        AllowedAttributeValuesDeleteRequest request = new AllowedAttributeValuesDeleteRequest(attributeValueListKey, NO_ALLOWED_ATTRIBUTE_VALUES);

        try
        {
            allowedAttributeValueService.deleteAllowedAttributeValues(request);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("At least one allowed attribute value must be specified.", e.getMessage());
        }

        request = new AllowedAttributeValuesDeleteRequest(attributeValueListKey, Arrays.asList(ALLOWED_ATTRIBUTE_VALUE, ALLOWED_ATTRIBUTE_VALUE));

        // Mock calls to external methods.
        when(alternateKeyHelper.validateStringParameter("An", "allowed attribute value", ALLOWED_ATTRIBUTE_VALUE)).thenReturn(ALLOWED_ATTRIBUTE_VALUE);

        try
        {
            allowedAttributeValueService.deleteAllowedAttributeValues(request);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Duplicate allowed attribute value \"%s\" found.", ALLOWED_ATTRIBUTE_VALUE), e.getMessage());
        }

        verify(alternateKeyHelper, times(2)).validateStringParameter("An", "allowed attribute value", ALLOWED_ATTRIBUTE_VALUE);
    }

    @Test
    public void testGetAllowedAttributeValues()
    {

        // Create attribute value list key.
        AttributeValueListKey attributeValueListKey = new AttributeValueListKey(NAMESPACE_CODE, ATTRIBUTE_VALUE_LIST_NAME);

        // Create namespace entity.
        NamespaceEntity namespaceEntity = new NamespaceEntity();
        namespaceEntity.setCode(NAMESPACE_CODE);

        // Create attribute value list entity.
        AttributeValueListEntity attributeValueListEntity = new AttributeValueListEntity();
        attributeValueListEntity.setId(ATTRIBUTE_VALUE_LIST_ID);
        attributeValueListEntity.setNamespace(namespaceEntity);
        attributeValueListEntity.setAttributeValueListName(ATTRIBUTE_VALUE_LIST_NAME);
        attributeValueListEntity.setAllowedAttributeValues(new ArrayList<>());

        // Create allowed attribute value entity.
        AllowedAttributeValueEntity allowedAttributeValueEntity = new AllowedAttributeValueEntity();
        allowedAttributeValueEntity.setAllowedAttributeValue(ALLOWED_ATTRIBUTE_VALUE);
        allowedAttributeValueEntity.setAttributeValueListEntity(attributeValueListEntity);

        List<AllowedAttributeValueEntity> allowedAttributeValueEntities = new ArrayList<>();
        allowedAttributeValueEntities.add(allowedAttributeValueEntity);

        // Mock calls to external method.
        when(attributeValueListDaoHelper.getAttributeValueListEntity(attributeValueListKey)).thenReturn(attributeValueListEntity);
        when(allowedAttributeValueDao.getAllowedAttributeValuesByAttributeValueListKey(attributeValueListKey)).thenReturn(allowedAttributeValueEntities);

        // Call method under test.
        AllowedAttributeValuesInformation response = allowedAttributeValueService.getAllowedAttributeValues(attributeValueListKey);

        // Verify the calls.
        verify(attributeValueListDaoHelper).getAttributeValueListEntity(attributeValueListKey);
        verify(allowedAttributeValueDao).getAllowedAttributeValuesByAttributeValueListKey(attributeValueListKey);

        // Validate the response.
        assertEquals(attributeValueListKey, response.getAttributeValueListKey());
        assertEquals(Arrays.asList(ALLOWED_ATTRIBUTE_VALUE), response.getAllowedAttributeValues());
    }
}

