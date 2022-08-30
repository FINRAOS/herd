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
package org.finra.herd.service.impl;

import static org.finra.herd.dao.AbstractDaoTest.ATTRIBUTE_NAME;
import static org.finra.herd.dao.AbstractDaoTest.ATTRIBUTE_NAME_2;
import static org.finra.herd.dao.AbstractDaoTest.ATTRIBUTE_NAME_3;
import static org.finra.herd.dao.AbstractDaoTest.ATTRIBUTE_VALUE;
import static org.finra.herd.dao.AbstractDaoTest.ATTRIBUTE_VALUE_2;
import static org.finra.herd.dao.AbstractDaoTest.ATTRIBUTE_VALUE_3;
import static org.finra.herd.dao.AbstractDaoTest.BDEF_NAME;
import static org.finra.herd.dao.AbstractDaoTest.BDEF_NAMESPACE;
import static org.finra.herd.dao.AbstractDaoTest.DATA_VERSION;
import static org.finra.herd.dao.AbstractDaoTest.FORMAT_FILE_TYPE_CODE;
import static org.finra.herd.dao.AbstractDaoTest.FORMAT_USAGE_CODE;
import static org.finra.herd.dao.AbstractDaoTest.FORMAT_VERSION;
import static org.finra.herd.dao.AbstractDaoTest.PARTITION_VALUE;
import static org.finra.herd.dao.AbstractDaoTest.SUBPARTITION_VALUES;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.dao.BusinessObjectDataAttributeDao;
import org.finra.herd.dao.BusinessObjectDataDao;
import org.finra.herd.model.api.xml.BusinessObjectDataAttribute;
import org.finra.herd.model.api.xml.BusinessObjectDataAttributeCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataAttributeKey;
import org.finra.herd.model.api.xml.BusinessObjectDataAttributeUpdateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.dto.AttributeDto;
import org.finra.herd.model.jpa.BusinessObjectDataAttributeEntity;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.service.MessageNotificationEventService;
import org.finra.herd.service.helper.BusinessObjectDataAttributeDaoHelper;
import org.finra.herd.service.helper.BusinessObjectDataAttributeHelper;
import org.finra.herd.service.helper.BusinessObjectDataDaoHelper;
import org.finra.herd.service.helper.BusinessObjectDataHelper;
import org.finra.herd.service.helper.BusinessObjectFormatDaoHelper;
import org.finra.herd.service.helper.BusinessObjectFormatHelper;

/**
 * This class tests functionality within the business object data service implementation.
 */
public class BusinessObjectDataAttributeServiceImplTest
{
    @Mock
    private BusinessObjectDataAttributeDao businessObjectDataAttributeDao;

    @Mock
    private BusinessObjectDataAttributeDaoHelper businessObjectDataAttributeDaoHelper;

    @Mock
    private BusinessObjectDataAttributeHelper businessObjectDataAttributeHelper;

    @Mock
    private BusinessObjectDataDao businessObjectDataDao;

    @Mock
    private BusinessObjectDataDaoHelper businessObjectDataDaoHelper;

    @Mock
    private BusinessObjectDataHelper businessObjectDataHelper;

    @Mock
    private BusinessObjectFormatDaoHelper businessObjectFormatDaoHelper;

    @Mock
    private BusinessObjectFormatHelper businessObjectFormatHelper;

    @Mock
    private MessageNotificationEventService messageNotificationEventService;

    @InjectMocks
    private BusinessObjectDataAttributeServiceImpl businessObjectDataAttributeServiceImpl;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCreateBusinessObjectDataAttribute()
    {
        // Create a business object data attribute key.
        BusinessObjectDataAttributeKey businessObjectDataAttributeKey =
            new BusinessObjectDataAttributeKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME);

        // Create a business object data create update request.
        BusinessObjectDataAttributeCreateRequest businessObjectDataAttributeCreateRequest =
            new BusinessObjectDataAttributeCreateRequest(businessObjectDataAttributeKey, ATTRIBUTE_VALUE);

        // Create business object format key.
        BusinessObjectFormatKey businessObjectFormatKey =
            new BusinessObjectFormatKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION);

        // Create business object format.
        BusinessObjectFormatEntity businessObjectFormatEntity = new BusinessObjectFormatEntity();

        // Create business object data.
        BusinessObjectDataEntity businessObjectDataEntity = new BusinessObjectDataEntity();
        businessObjectDataEntity.setAttributes(Collections.singletonList(new BusinessObjectDataAttributeEntity()));

        // Create business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a map of business object data attributes.
        Map<String, BusinessObjectDataAttributeEntity> businessObjectDataAttributeEntityMap = new HashMap<>();

        // Create a business object data attribute entity.
        BusinessObjectDataAttributeEntity businessObjectDataAttributeEntity = new BusinessObjectDataAttributeEntity();

        // Create business object data attribute.
        BusinessObjectDataAttribute businessObjectDataAttribute = new BusinessObjectDataAttribute();

        // Mock the external calls.
        when(businessObjectFormatDaoHelper.getBusinessObjectFormatEntity(businessObjectFormatKey)).thenReturn(businessObjectFormatEntity);
        when(businessObjectDataAttributeHelper.isBusinessObjectDataAttributeRequired(ATTRIBUTE_NAME, businessObjectFormatEntity)).thenReturn(false);
        when(businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataKey)).thenReturn(businessObjectDataEntity);
        when(businessObjectDataAttributeDaoHelper.getBusinessObjectDataAttributeEntityMap(businessObjectDataEntity.getAttributes()))
            .thenReturn(businessObjectDataAttributeEntityMap);
        when(businessObjectFormatDaoHelper.isBusinessObjectDataPublishedAttributesChangeEventNotificationRequired(businessObjectFormatEntity))
            .thenReturn(false);
        when(businessObjectDataAttributeDao.saveAndRefresh(any(BusinessObjectDataAttributeEntity.class))).thenReturn(businessObjectDataAttributeEntity);
        when(businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity)).thenReturn(businessObjectDataKey);
        when(businessObjectDataAttributeDaoHelper.createBusinessObjectDataAttributeFromEntity(businessObjectDataAttributeEntity))
            .thenReturn(businessObjectDataAttribute);

        // Call the method under test.
        BusinessObjectDataAttribute result =
            businessObjectDataAttributeServiceImpl.createBusinessObjectDataAttributeImpl(businessObjectDataAttributeCreateRequest);

        // Validate the results.
        assertEquals(businessObjectDataAttribute, result);

        // Verify the external calls.
        verify(businessObjectDataAttributeHelper).validateBusinessObjectDataAttributeKey(businessObjectDataAttributeKey);
        verify(businessObjectFormatDaoHelper).getBusinessObjectFormatEntity(businessObjectFormatKey);
        verify(businessObjectDataAttributeHelper).isBusinessObjectDataAttributeRequired(ATTRIBUTE_NAME, businessObjectFormatEntity);
        verify(businessObjectDataDaoHelper).getBusinessObjectDataEntity(businessObjectDataKey);
        verify(businessObjectDataAttributeDaoHelper).getBusinessObjectDataAttributeEntityMap(businessObjectDataEntity.getAttributes());
        verify(businessObjectFormatDaoHelper).isBusinessObjectDataPublishedAttributesChangeEventNotificationRequired(businessObjectFormatEntity);
        verify(businessObjectDataAttributeDao).saveAndRefresh(any(BusinessObjectDataAttributeEntity.class));
        verify(businessObjectDataAttributeDaoHelper).createBusinessObjectDataAttributeFromEntity(businessObjectDataAttributeEntity);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testCreateBusinessObjectDataAttributeWithBusinessObjectDataAttributesChangeNotification()
    {
        // Create a business object data attribute key.
        BusinessObjectDataAttributeKey businessObjectDataAttributeKey =
            new BusinessObjectDataAttributeKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME);

        // Create a business object data create update request.
        BusinessObjectDataAttributeCreateRequest businessObjectDataAttributeCreateRequest =
            new BusinessObjectDataAttributeCreateRequest(businessObjectDataAttributeKey, ATTRIBUTE_VALUE);

        // Create business object format key.
        BusinessObjectFormatKey businessObjectFormatKey =
            new BusinessObjectFormatKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION);

        // Create business object format.
        BusinessObjectFormatEntity businessObjectFormatEntity = new BusinessObjectFormatEntity();

        // Create business object data.
        BusinessObjectDataEntity businessObjectDataEntity = new BusinessObjectDataEntity();
        businessObjectDataEntity.setAttributes(Collections.singletonList(new BusinessObjectDataAttributeEntity()));

        // Create business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a map of business object data attributes.
        Map<String, BusinessObjectDataAttributeEntity> businessObjectDataAttributeEntityMap = new HashMap<>();

        // Create a business object data attribute entity.
        BusinessObjectDataAttributeEntity businessObjectDataAttributeEntity = new BusinessObjectDataAttributeEntity();
        businessObjectDataAttributeEntity.setBusinessObjectData(businessObjectDataEntity);

        // Create business object data attribute.
        BusinessObjectDataAttribute businessObjectDataAttribute = new BusinessObjectDataAttribute();

        // Create a list of old published business object data attributes.
        List<AttributeDto> oldPublishedBusinessObjectDataAttributes =
            Arrays.asList(new AttributeDto(ATTRIBUTE_NAME_2, ATTRIBUTE_VALUE_2), new AttributeDto(ATTRIBUTE_NAME_3, ATTRIBUTE_VALUE_3));

        // Mock the external calls.
        when(businessObjectFormatDaoHelper.getBusinessObjectFormatEntity(businessObjectFormatKey)).thenReturn(businessObjectFormatEntity);
        when(businessObjectDataAttributeHelper.isBusinessObjectDataAttributeRequired(ATTRIBUTE_NAME, businessObjectFormatEntity)).thenReturn(false);
        when(businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataKey)).thenReturn(businessObjectDataEntity);
        when(businessObjectDataAttributeDaoHelper.getBusinessObjectDataAttributeEntityMap(businessObjectDataEntity.getAttributes()))
            .thenReturn(businessObjectDataAttributeEntityMap);
        when(businessObjectFormatDaoHelper.isBusinessObjectDataPublishedAttributesChangeEventNotificationRequired(businessObjectFormatEntity)).thenReturn(true);
        when(businessObjectDataDaoHelper.getPublishedBusinessObjectDataAttributes(businessObjectDataEntity))
            .thenReturn(oldPublishedBusinessObjectDataAttributes);
        when(businessObjectDataAttributeDao.saveAndRefresh(any(BusinessObjectDataAttributeEntity.class))).thenReturn(businessObjectDataAttributeEntity);
        when(businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity)).thenReturn(businessObjectDataKey);
        when(businessObjectDataAttributeDaoHelper.createBusinessObjectDataAttributeFromEntity(businessObjectDataAttributeEntity))
            .thenReturn(businessObjectDataAttribute);

        // Call the method under test.
        BusinessObjectDataAttribute result =
            businessObjectDataAttributeServiceImpl.createBusinessObjectDataAttributeImpl(businessObjectDataAttributeCreateRequest);

        // Validate the results.
        assertEquals(businessObjectDataAttribute, result);

        // Verify the external calls.
        verify(businessObjectDataAttributeHelper).validateBusinessObjectDataAttributeKey(businessObjectDataAttributeKey);
        verify(businessObjectFormatDaoHelper).getBusinessObjectFormatEntity(businessObjectFormatKey);
        verify(businessObjectDataAttributeHelper).isBusinessObjectDataAttributeRequired(ATTRIBUTE_NAME, businessObjectFormatEntity);
        verify(businessObjectDataDaoHelper).getBusinessObjectDataEntity(businessObjectDataKey);
        verify(businessObjectDataAttributeDaoHelper).getBusinessObjectDataAttributeEntityMap(businessObjectDataEntity.getAttributes());
        verify(businessObjectFormatDaoHelper).isBusinessObjectDataPublishedAttributesChangeEventNotificationRequired(businessObjectFormatEntity);
        verify(businessObjectDataDaoHelper).getPublishedBusinessObjectDataAttributes(businessObjectDataEntity);
        verify(businessObjectDataAttributeDao).saveAndRefresh(any(BusinessObjectDataAttributeEntity.class));
        verify(businessObjectDataHelper).getBusinessObjectDataKey(businessObjectDataEntity);
        verify(messageNotificationEventService)
            .processBusinessObjectDataPublishedAttributesChangeNotificationEvent(businessObjectDataKey, oldPublishedBusinessObjectDataAttributes);
        verify(businessObjectDataAttributeDaoHelper).createBusinessObjectDataAttributeFromEntity(businessObjectDataAttributeEntity);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testDeleteBusinessObjectDataAttribute()
    {
        // Create a business object data attribute key.
        BusinessObjectDataAttributeKey businessObjectDataAttributeKey =
            new BusinessObjectDataAttributeKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME);

        // Create business object format key.
        BusinessObjectFormatKey businessObjectFormatKey =
            new BusinessObjectFormatKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION);

        // Create business object format.
        BusinessObjectFormatEntity businessObjectFormatEntity = new BusinessObjectFormatEntity();

        // Create business object data.
        BusinessObjectDataEntity businessObjectDataEntity = new BusinessObjectDataEntity();
        businessObjectDataEntity.setAttributes(Collections.singletonList(new BusinessObjectDataAttributeEntity()));

        // Create a business object data attribute entity.
        BusinessObjectDataAttributeEntity businessObjectDataAttributeEntity = new BusinessObjectDataAttributeEntity();
        businessObjectDataAttributeEntity.setBusinessObjectData(businessObjectDataEntity);

        // Create business object data attribute.
        BusinessObjectDataAttribute businessObjectDataAttribute = new BusinessObjectDataAttribute();

        // Mock the external calls.
        when(businessObjectFormatDaoHelper.getBusinessObjectFormatEntity(businessObjectFormatKey)).thenReturn(businessObjectFormatEntity);
        when(businessObjectDataAttributeHelper.isBusinessObjectDataAttributeRequired(ATTRIBUTE_NAME, businessObjectFormatEntity)).thenReturn(false);
        when(businessObjectDataAttributeDaoHelper.getBusinessObjectDataAttributeEntity(businessObjectDataAttributeKey))
            .thenReturn(businessObjectDataAttributeEntity);
        when(businessObjectFormatDaoHelper.isBusinessObjectDataPublishedAttributesChangeEventNotificationRequired(businessObjectFormatEntity))
            .thenReturn(false);
        when(businessObjectDataDao.saveAndRefresh(businessObjectDataEntity)).thenReturn(businessObjectDataEntity);
        when(businessObjectDataAttributeDaoHelper.createBusinessObjectDataAttributeFromEntity(businessObjectDataAttributeEntity))
            .thenReturn(businessObjectDataAttribute);

        // Call the method under test.
        BusinessObjectDataAttribute result = businessObjectDataAttributeServiceImpl.deleteBusinessObjectDataAttributeImpl(businessObjectDataAttributeKey);

        // Validate the results.
        assertEquals(businessObjectDataAttribute, result);

        // Verify the external calls.
        verify(businessObjectDataAttributeHelper).validateBusinessObjectDataAttributeKey(businessObjectDataAttributeKey);
        verify(businessObjectFormatDaoHelper).getBusinessObjectFormatEntity(businessObjectFormatKey);
        verify(businessObjectDataAttributeHelper).isBusinessObjectDataAttributeRequired(ATTRIBUTE_NAME, businessObjectFormatEntity);
        verify(businessObjectDataAttributeDaoHelper).getBusinessObjectDataAttributeEntity(businessObjectDataAttributeKey);
        verify(businessObjectFormatDaoHelper).isBusinessObjectDataPublishedAttributesChangeEventNotificationRequired(businessObjectFormatEntity);
        verify(businessObjectDataDao).saveAndRefresh(businessObjectDataEntity);
        verify(businessObjectDataAttributeDaoHelper).createBusinessObjectDataAttributeFromEntity(businessObjectDataAttributeEntity);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testDeleteBusinessObjectDataAttributeWithBusinessObjectDataAttributesChangeNotification()
    {
        // Create a business object data attribute key.
        BusinessObjectDataAttributeKey businessObjectDataAttributeKey =
            new BusinessObjectDataAttributeKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME);

        // Create business object format key.
        BusinessObjectFormatKey businessObjectFormatKey =
            new BusinessObjectFormatKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION);

        // Create business object format.
        BusinessObjectFormatEntity businessObjectFormatEntity = new BusinessObjectFormatEntity();

        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create business object data.
        BusinessObjectDataEntity businessObjectDataEntity = new BusinessObjectDataEntity();
        businessObjectDataEntity.setAttributes(Collections.singletonList(new BusinessObjectDataAttributeEntity()));

        // Create a business object data attribute entity.
        BusinessObjectDataAttributeEntity businessObjectDataAttributeEntity = new BusinessObjectDataAttributeEntity();
        businessObjectDataAttributeEntity.setBusinessObjectData(businessObjectDataEntity);

        // Create business object data attribute.
        BusinessObjectDataAttribute businessObjectDataAttribute = new BusinessObjectDataAttribute();

        // Create a list of old published business object data attributes.
        List<AttributeDto> oldPublishedBusinessObjectDataAttributes =
            Arrays.asList(new AttributeDto(ATTRIBUTE_NAME_2, ATTRIBUTE_VALUE_2), new AttributeDto(ATTRIBUTE_NAME_3, ATTRIBUTE_VALUE_3));


        // Mock the external calls.
        when(businessObjectFormatDaoHelper.getBusinessObjectFormatEntity(businessObjectFormatKey)).thenReturn(businessObjectFormatEntity);
        when(businessObjectDataAttributeHelper.isBusinessObjectDataAttributeRequired(ATTRIBUTE_NAME, businessObjectFormatEntity)).thenReturn(false);
        when(businessObjectDataAttributeDaoHelper.getBusinessObjectDataAttributeEntity(businessObjectDataAttributeKey))
            .thenReturn(businessObjectDataAttributeEntity);
        when(businessObjectFormatDaoHelper.isBusinessObjectDataPublishedAttributesChangeEventNotificationRequired(businessObjectFormatEntity)).thenReturn(true);
        when(businessObjectDataDaoHelper.getPublishedBusinessObjectDataAttributes(businessObjectDataEntity))
            .thenReturn(oldPublishedBusinessObjectDataAttributes);
        when(businessObjectDataDao.saveAndRefresh(businessObjectDataEntity)).thenReturn(businessObjectDataEntity);
        when(businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity)).thenReturn(businessObjectDataKey);
        when(businessObjectDataAttributeDaoHelper.createBusinessObjectDataAttributeFromEntity(businessObjectDataAttributeEntity))
            .thenReturn(businessObjectDataAttribute);

        // Call the method under test.
        BusinessObjectDataAttribute result = businessObjectDataAttributeServiceImpl.deleteBusinessObjectDataAttributeImpl(businessObjectDataAttributeKey);

        // Validate the results.
        assertEquals(businessObjectDataAttribute, result);

        // Verify the external calls.
        verify(businessObjectDataAttributeHelper).validateBusinessObjectDataAttributeKey(businessObjectDataAttributeKey);
        verify(businessObjectFormatDaoHelper).getBusinessObjectFormatEntity(businessObjectFormatKey);
        verify(businessObjectDataAttributeHelper).isBusinessObjectDataAttributeRequired(ATTRIBUTE_NAME, businessObjectFormatEntity);
        verify(businessObjectDataAttributeDaoHelper).getBusinessObjectDataAttributeEntity(businessObjectDataAttributeKey);
        verify(businessObjectFormatDaoHelper).isBusinessObjectDataPublishedAttributesChangeEventNotificationRequired(businessObjectFormatEntity);
        verify(businessObjectDataDaoHelper).getPublishedBusinessObjectDataAttributes(businessObjectDataEntity);
        verify(businessObjectDataDao).saveAndRefresh(businessObjectDataEntity);
        verify(businessObjectDataHelper).getBusinessObjectDataKey(businessObjectDataEntity);
        verify(messageNotificationEventService)
            .processBusinessObjectDataPublishedAttributesChangeNotificationEvent(businessObjectDataKey, oldPublishedBusinessObjectDataAttributes);
        verify(businessObjectDataAttributeDaoHelper).createBusinessObjectDataAttributeFromEntity(businessObjectDataAttributeEntity);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testUpdateBusinessObjectDataAttribute()
    {
        // Create a business object data attribute key.
        BusinessObjectDataAttributeKey businessObjectDataAttributeKey =
            new BusinessObjectDataAttributeKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME);

        // Create a business object data attribute update request.
        BusinessObjectDataAttributeUpdateRequest businessObjectDataAttributeUpdateRequest = new BusinessObjectDataAttributeUpdateRequest(ATTRIBUTE_VALUE);

        // Create business object format key.
        BusinessObjectFormatKey businessObjectFormatKey =
            new BusinessObjectFormatKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION);

        // Create a business object format definition.
        BusinessObjectFormatEntity businessObjectFormatEntity = new BusinessObjectFormatEntity();

        // Create a business object data attribute entity.
        BusinessObjectDataAttributeEntity businessObjectDataAttributeEntity = new BusinessObjectDataAttributeEntity();

        // Create business object data attribute.
        BusinessObjectDataAttribute businessObjectDataAttribute = new BusinessObjectDataAttribute();

        // Mock the external calls.
        when(businessObjectFormatDaoHelper.getBusinessObjectFormatEntity(businessObjectFormatKey)).thenReturn(businessObjectFormatEntity);
        when(businessObjectDataAttributeHelper.isBusinessObjectDataAttributeRequired(ATTRIBUTE_NAME, businessObjectFormatEntity)).thenReturn(false);
        when(businessObjectDataAttributeDaoHelper.getBusinessObjectDataAttributeEntity(businessObjectDataAttributeKey))
            .thenReturn(businessObjectDataAttributeEntity);
        when(businessObjectFormatDaoHelper.isBusinessObjectDataPublishedAttributesChangeEventNotificationRequired(businessObjectFormatEntity))
            .thenReturn(false);
        when(businessObjectDataAttributeDao.saveAndRefresh(businessObjectDataAttributeEntity)).thenReturn(businessObjectDataAttributeEntity);
        when(businessObjectDataAttributeDaoHelper.createBusinessObjectDataAttributeFromEntity(businessObjectDataAttributeEntity))
            .thenReturn(businessObjectDataAttribute);

        // Call the method under test.
        BusinessObjectDataAttribute result = businessObjectDataAttributeServiceImpl
            .updateBusinessObjectDataAttributeImpl(businessObjectDataAttributeKey, businessObjectDataAttributeUpdateRequest);

        // Validate the results.
        assertEquals(businessObjectDataAttribute, result);

        // Verify the external calls.
        verify(businessObjectDataAttributeHelper).validateBusinessObjectDataAttributeKey(businessObjectDataAttributeKey);
        verify(businessObjectFormatDaoHelper).getBusinessObjectFormatEntity(businessObjectFormatKey);
        verify(businessObjectDataAttributeHelper).isBusinessObjectDataAttributeRequired(ATTRIBUTE_NAME, businessObjectFormatEntity);
        verify(businessObjectDataAttributeDaoHelper).getBusinessObjectDataAttributeEntity(businessObjectDataAttributeKey);
        verify(businessObjectFormatDaoHelper).isBusinessObjectDataPublishedAttributesChangeEventNotificationRequired(businessObjectFormatEntity);
        verify(businessObjectDataAttributeDao).saveAndRefresh(businessObjectDataAttributeEntity);
        verify(businessObjectDataAttributeDaoHelper).createBusinessObjectDataAttributeFromEntity(businessObjectDataAttributeEntity);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testUpdateBusinessObjectDataAttributeWithBusinessObjectDataAttributesChangeNotification()
    {
        // Create a business object data attribute key.
        BusinessObjectDataAttributeKey businessObjectDataAttributeKey =
            new BusinessObjectDataAttributeKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME);

        // Create a business object data attribute update request.
        BusinessObjectDataAttributeUpdateRequest businessObjectDataAttributeUpdateRequest = new BusinessObjectDataAttributeUpdateRequest(ATTRIBUTE_VALUE);

        // Create business object format key.
        BusinessObjectFormatKey businessObjectFormatKey =
            new BusinessObjectFormatKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION);

        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a business object format definition.
        BusinessObjectFormatEntity businessObjectFormatEntity = new BusinessObjectFormatEntity();

        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity = new BusinessObjectDataEntity();

        // Create a business object data attribute entity.
        BusinessObjectDataAttributeEntity businessObjectDataAttributeEntity = new BusinessObjectDataAttributeEntity();
        businessObjectDataAttributeEntity.setBusinessObjectData(businessObjectDataEntity);

        // Create business object data attribute.
        BusinessObjectDataAttribute businessObjectDataAttribute = new BusinessObjectDataAttribute();

        // Create a list of old published business object data attributes.
        List<AttributeDto> oldPublishedBusinessObjectDataAttributes =
            Arrays.asList(new AttributeDto(ATTRIBUTE_NAME_2, ATTRIBUTE_VALUE_2), new AttributeDto(ATTRIBUTE_NAME_3, ATTRIBUTE_VALUE_3));

        // Mock the external calls.
        when(businessObjectFormatDaoHelper.getBusinessObjectFormatEntity(businessObjectFormatKey)).thenReturn(businessObjectFormatEntity);
        when(businessObjectDataAttributeHelper.isBusinessObjectDataAttributeRequired(ATTRIBUTE_NAME, businessObjectFormatEntity)).thenReturn(false);
        when(businessObjectDataAttributeDaoHelper.getBusinessObjectDataAttributeEntity(businessObjectDataAttributeKey))
            .thenReturn(businessObjectDataAttributeEntity);
        when(businessObjectFormatDaoHelper.isBusinessObjectDataPublishedAttributesChangeEventNotificationRequired(businessObjectFormatEntity)).thenReturn(true);
        when(businessObjectDataDaoHelper.getPublishedBusinessObjectDataAttributes(businessObjectDataEntity))
            .thenReturn(oldPublishedBusinessObjectDataAttributes);
        when(businessObjectDataAttributeDao.saveAndRefresh(businessObjectDataAttributeEntity)).thenReturn(businessObjectDataAttributeEntity);
        when(businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity)).thenReturn(businessObjectDataKey);
        when(businessObjectDataAttributeDaoHelper.createBusinessObjectDataAttributeFromEntity(businessObjectDataAttributeEntity))
            .thenReturn(businessObjectDataAttribute);

        // Call the method under test.
        BusinessObjectDataAttribute result = businessObjectDataAttributeServiceImpl
            .updateBusinessObjectDataAttributeImpl(businessObjectDataAttributeKey, businessObjectDataAttributeUpdateRequest);

        // Validate the results.
        assertEquals(businessObjectDataAttribute, result);

        // Verify the external calls.
        verify(businessObjectDataAttributeHelper).validateBusinessObjectDataAttributeKey(businessObjectDataAttributeKey);
        verify(businessObjectFormatDaoHelper).getBusinessObjectFormatEntity(businessObjectFormatKey);
        verify(businessObjectDataAttributeHelper).isBusinessObjectDataAttributeRequired(ATTRIBUTE_NAME, businessObjectFormatEntity);
        verify(businessObjectDataAttributeDaoHelper).getBusinessObjectDataAttributeEntity(businessObjectDataAttributeKey);
        verify(businessObjectFormatDaoHelper).isBusinessObjectDataPublishedAttributesChangeEventNotificationRequired(businessObjectFormatEntity);
        verify(businessObjectDataDaoHelper).getPublishedBusinessObjectDataAttributes(businessObjectDataEntity);
        verify(businessObjectDataAttributeDao).saveAndRefresh(businessObjectDataAttributeEntity);
        verify(businessObjectDataHelper).getBusinessObjectDataKey(businessObjectDataEntity);
        verify(messageNotificationEventService)
            .processBusinessObjectDataPublishedAttributesChangeNotificationEvent(businessObjectDataKey, oldPublishedBusinessObjectDataAttributes);
        verify(businessObjectDataAttributeDaoHelper).createBusinessObjectDataAttributeFromEntity(businessObjectDataAttributeEntity);
        verifyNoMoreInteractionsHelper();
    }

    /**
     * Checks if any of the mocks has any interaction.
     */
    private void verifyNoMoreInteractionsHelper()
    {
        verifyNoMoreInteractions(businessObjectDataAttributeDao, businessObjectDataAttributeDaoHelper, businessObjectDataAttributeHelper, businessObjectDataDao,
            businessObjectDataHelper, businessObjectDataDaoHelper, businessObjectFormatDaoHelper, businessObjectFormatHelper, messageNotificationEventService);
    }
}
