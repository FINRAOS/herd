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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.model.api.xml.BusinessObjectDataAttribute;
import org.finra.herd.model.api.xml.BusinessObjectDataAttributeCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataAttributeKey;
import org.finra.herd.model.api.xml.BusinessObjectDataAttributeKeys;
import org.finra.herd.model.api.xml.BusinessObjectDataAttributeUpdateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.service.BusinessObjectDataAttributeService;

/**
 * This class tests various functionality within the business object data attribute REST controller.
 */
public class BusinessObjectDataAttributeRestControllerTest extends AbstractRestTest
{
    @InjectMocks
    private BusinessObjectDataAttributeRestController businessObjectDataAttributeRestController;

    @Mock
    private BusinessObjectDataAttributeService businessObjectDataAttributeService;

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
                NO_SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE);

        // Create a business object data create request.
        BusinessObjectDataAttributeCreateRequest businessObjectDataAttributeCreateRequest =
            new BusinessObjectDataAttributeCreateRequest(businessObjectDataAttributeKey, ATTRIBUTE_VALUE_1);

        // Create a business object data attribute.
        BusinessObjectDataAttribute businessObjectDataAttribute = new BusinessObjectDataAttribute(ID, businessObjectDataAttributeKey, ATTRIBUTE_VALUE_1);

        // Mock the external calls.
        when(businessObjectDataAttributeService.createBusinessObjectDataAttribute(businessObjectDataAttributeCreateRequest))
            .thenReturn(businessObjectDataAttribute);

        // Call the method under test.
        BusinessObjectDataAttribute result =
            businessObjectDataAttributeRestController.createBusinessObjectDataAttribute(businessObjectDataAttributeCreateRequest);

        // Verify the external calls.
        verify(businessObjectDataAttributeService).createBusinessObjectDataAttribute(businessObjectDataAttributeCreateRequest);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(businessObjectDataAttribute, result);
    }

    @Test
    public void testDeleteBusinessObjectDataAttributeSubPartitionValuesCount0()
    {
        // Create a business object data attribute key.
        BusinessObjectDataAttributeKey businessObjectDataAttributeKey =
            new BusinessObjectDataAttributeKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE);

        // Create a business object data attribute.
        BusinessObjectDataAttribute businessObjectDataAttribute = new BusinessObjectDataAttribute(ID, businessObjectDataAttributeKey, ATTRIBUTE_VALUE_1);

        // Mock the external calls.
        when(businessObjectDataAttributeService.deleteBusinessObjectDataAttribute(businessObjectDataAttributeKey)).thenReturn(businessObjectDataAttribute);

        // Call the method under test.
        BusinessObjectDataAttribute result = businessObjectDataAttributeRestController
            .deleteBusinessObjectDataAttribute(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE);

        // Verify the external calls.
        verify(businessObjectDataAttributeService).deleteBusinessObjectDataAttribute(businessObjectDataAttributeKey);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(businessObjectDataAttribute, result);
    }

    @Test
    public void testDeleteBusinessObjectDataAttributeSubPartitionValuesCount1()
    {
        // Create a business object data attribute key.
        BusinessObjectDataAttributeKey businessObjectDataAttributeKey =
            new BusinessObjectDataAttributeKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                Arrays.asList(SUBPARTITION_VALUES.get(0)), DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE);

        // Create a business object data attribute.
        BusinessObjectDataAttribute businessObjectDataAttribute = new BusinessObjectDataAttribute(ID, businessObjectDataAttributeKey, ATTRIBUTE_VALUE_1);

        // Mock the external calls.
        when(businessObjectDataAttributeService.deleteBusinessObjectDataAttribute(businessObjectDataAttributeKey)).thenReturn(businessObjectDataAttribute);

        // Call the method under test.
        BusinessObjectDataAttribute result = businessObjectDataAttributeRestController
            .deleteBusinessObjectDataAttribute(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES.get(0), DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE);

        // Verify the external calls.
        verify(businessObjectDataAttributeService).deleteBusinessObjectDataAttribute(businessObjectDataAttributeKey);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(businessObjectDataAttribute, result);
    }

    @Test
    public void testDeleteBusinessObjectDataAttributeSubPartitionValuesCount2()
    {
        // Create a business object data attribute key.
        BusinessObjectDataAttributeKey businessObjectDataAttributeKey =
            new BusinessObjectDataAttributeKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                Arrays.asList(SUBPARTITION_VALUES.get(0), SUBPARTITION_VALUES.get(1)), DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE);

        // Create a business object data attribute.
        BusinessObjectDataAttribute businessObjectDataAttribute = new BusinessObjectDataAttribute(ID, businessObjectDataAttributeKey, ATTRIBUTE_VALUE_1);

        // Mock the external calls.
        when(businessObjectDataAttributeService.deleteBusinessObjectDataAttribute(businessObjectDataAttributeKey)).thenReturn(businessObjectDataAttribute);

        // Call the method under test.
        BusinessObjectDataAttribute result = businessObjectDataAttributeRestController
            .deleteBusinessObjectDataAttribute(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES.get(0), SUBPARTITION_VALUES.get(1), DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE);

        // Verify the external calls.
        verify(businessObjectDataAttributeService).deleteBusinessObjectDataAttribute(businessObjectDataAttributeKey);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(businessObjectDataAttribute, result);
    }

    @Test
    public void testDeleteBusinessObjectDataAttributeSubPartitionValuesCount3()
    {
        // Create a business object data attribute key.
        BusinessObjectDataAttributeKey businessObjectDataAttributeKey =
            new BusinessObjectDataAttributeKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                Arrays.asList(SUBPARTITION_VALUES.get(0), SUBPARTITION_VALUES.get(1), SUBPARTITION_VALUES.get(2)), DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE);

        // Create a business object data attribute.
        BusinessObjectDataAttribute businessObjectDataAttribute = new BusinessObjectDataAttribute(ID, businessObjectDataAttributeKey, ATTRIBUTE_VALUE_1);

        // Mock the external calls.
        when(businessObjectDataAttributeService.deleteBusinessObjectDataAttribute(businessObjectDataAttributeKey)).thenReturn(businessObjectDataAttribute);

        // Call the method under test.
        BusinessObjectDataAttribute result = businessObjectDataAttributeRestController
            .deleteBusinessObjectDataAttribute(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES.get(0), SUBPARTITION_VALUES.get(1), SUBPARTITION_VALUES.get(2), DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE);

        // Verify the external calls.
        verify(businessObjectDataAttributeService).deleteBusinessObjectDataAttribute(businessObjectDataAttributeKey);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(businessObjectDataAttribute, result);
    }

    @Test
    public void testDeleteBusinessObjectDataAttributeSubPartitionValuesCount4()
    {
        // Create a business object data attribute key.
        BusinessObjectDataAttributeKey businessObjectDataAttributeKey =
            new BusinessObjectDataAttributeKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                Arrays.asList(SUBPARTITION_VALUES.get(0), SUBPARTITION_VALUES.get(1), SUBPARTITION_VALUES.get(2), SUBPARTITION_VALUES.get(3)), DATA_VERSION,
                ATTRIBUTE_NAME_1_MIXED_CASE);

        // Create a business object data attribute.
        BusinessObjectDataAttribute businessObjectDataAttribute = new BusinessObjectDataAttribute(ID, businessObjectDataAttributeKey, ATTRIBUTE_VALUE_1);

        // Mock the external calls.
        when(businessObjectDataAttributeService.deleteBusinessObjectDataAttribute(businessObjectDataAttributeKey)).thenReturn(businessObjectDataAttribute);

        // Call the method under test.
        BusinessObjectDataAttribute result = businessObjectDataAttributeRestController
            .deleteBusinessObjectDataAttribute(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES.get(0), SUBPARTITION_VALUES.get(1), SUBPARTITION_VALUES.get(2), SUBPARTITION_VALUES.get(3), DATA_VERSION,
                ATTRIBUTE_NAME_1_MIXED_CASE);

        // Verify the external calls.
        verify(businessObjectDataAttributeService).deleteBusinessObjectDataAttribute(businessObjectDataAttributeKey);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(businessObjectDataAttribute, result);
    }

    @Test
    public void testGetBusinessObjectDataAttributeSubPartitionValuesCount0()
    {
        // Create a business object data attribute key.
        BusinessObjectDataAttributeKey businessObjectDataAttributeKey =
            new BusinessObjectDataAttributeKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE);

        // Create a business object data attribute.
        BusinessObjectDataAttribute businessObjectDataAttribute = new BusinessObjectDataAttribute(ID, businessObjectDataAttributeKey, ATTRIBUTE_VALUE_1);

        // Mock the external calls.
        when(businessObjectDataAttributeService.getBusinessObjectDataAttribute(businessObjectDataAttributeKey)).thenReturn(businessObjectDataAttribute);

        // Call the method under test.
        BusinessObjectDataAttribute result = businessObjectDataAttributeRestController
            .getBusinessObjectDataAttribute(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, DATA_VERSION,
                ATTRIBUTE_NAME_1_MIXED_CASE);

        // Verify the external calls.
        verify(businessObjectDataAttributeService).getBusinessObjectDataAttribute(businessObjectDataAttributeKey);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(businessObjectDataAttribute, result);
    }

    @Test
    public void testGetBusinessObjectDataAttributeSubPartitionValuesCount1()
    {
        // Create a business object data attribute key.
        BusinessObjectDataAttributeKey businessObjectDataAttributeKey =
            new BusinessObjectDataAttributeKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                Arrays.asList(SUBPARTITION_VALUES.get(0)), DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE);

        // Create a business object data attribute.
        BusinessObjectDataAttribute businessObjectDataAttribute = new BusinessObjectDataAttribute(ID, businessObjectDataAttributeKey, ATTRIBUTE_VALUE_1);

        // Mock the external calls.
        when(businessObjectDataAttributeService.getBusinessObjectDataAttribute(businessObjectDataAttributeKey)).thenReturn(businessObjectDataAttribute);

        // Call the method under test.
        BusinessObjectDataAttribute result = businessObjectDataAttributeRestController
            .getBusinessObjectDataAttribute(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES.get(0), DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE);

        // Verify the external calls.
        verify(businessObjectDataAttributeService).getBusinessObjectDataAttribute(businessObjectDataAttributeKey);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(businessObjectDataAttribute, result);
    }

    @Test
    public void testGetBusinessObjectDataAttributeSubPartitionValuesCount2()
    {
        // Create a business object data attribute key.
        BusinessObjectDataAttributeKey businessObjectDataAttributeKey =
            new BusinessObjectDataAttributeKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                Arrays.asList(SUBPARTITION_VALUES.get(0), SUBPARTITION_VALUES.get(1)), DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE);

        // Create a business object data attribute.
        BusinessObjectDataAttribute businessObjectDataAttribute = new BusinessObjectDataAttribute(ID, businessObjectDataAttributeKey, ATTRIBUTE_VALUE_1);

        // Mock the external calls.
        when(businessObjectDataAttributeService.getBusinessObjectDataAttribute(businessObjectDataAttributeKey)).thenReturn(businessObjectDataAttribute);

        // Call the method under test.
        BusinessObjectDataAttribute result = businessObjectDataAttributeRestController
            .getBusinessObjectDataAttribute(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES.get(0), SUBPARTITION_VALUES.get(1), DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE);

        // Verify the external calls.
        verify(businessObjectDataAttributeService).getBusinessObjectDataAttribute(businessObjectDataAttributeKey);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(businessObjectDataAttribute, result);
    }

    @Test
    public void testGetBusinessObjectDataAttributeSubPartitionValuesCount3()
    {
        // Create a business object data attribute key.
        BusinessObjectDataAttributeKey businessObjectDataAttributeKey =
            new BusinessObjectDataAttributeKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                Arrays.asList(SUBPARTITION_VALUES.get(0), SUBPARTITION_VALUES.get(1), SUBPARTITION_VALUES.get(2)), DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE);

        // Create a business object data attribute.
        BusinessObjectDataAttribute businessObjectDataAttribute = new BusinessObjectDataAttribute(ID, businessObjectDataAttributeKey, ATTRIBUTE_VALUE_1);

        // Mock the external calls.
        when(businessObjectDataAttributeService.getBusinessObjectDataAttribute(businessObjectDataAttributeKey)).thenReturn(businessObjectDataAttribute);

        // Call the method under test.
        BusinessObjectDataAttribute result = businessObjectDataAttributeRestController
            .getBusinessObjectDataAttribute(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES.get(0), SUBPARTITION_VALUES.get(1), SUBPARTITION_VALUES.get(2), DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE);

        // Verify the external calls.
        verify(businessObjectDataAttributeService).getBusinessObjectDataAttribute(businessObjectDataAttributeKey);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(businessObjectDataAttribute, result);
    }

    @Test
    public void testGetBusinessObjectDataAttributeSubPartitionValuesCount4()
    {
        // Create a business object data attribute key.
        BusinessObjectDataAttributeKey businessObjectDataAttributeKey =
            new BusinessObjectDataAttributeKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                Arrays.asList(SUBPARTITION_VALUES.get(0), SUBPARTITION_VALUES.get(1), SUBPARTITION_VALUES.get(2), SUBPARTITION_VALUES.get(3)), DATA_VERSION,
                ATTRIBUTE_NAME_1_MIXED_CASE);

        // Create a business object data attribute.
        BusinessObjectDataAttribute businessObjectDataAttribute = new BusinessObjectDataAttribute(ID, businessObjectDataAttributeKey, ATTRIBUTE_VALUE_1);

        // Mock the external calls.
        when(businessObjectDataAttributeService.getBusinessObjectDataAttribute(businessObjectDataAttributeKey)).thenReturn(businessObjectDataAttribute);

        // Call the method under test.
        BusinessObjectDataAttribute result = businessObjectDataAttributeRestController
            .getBusinessObjectDataAttribute(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES.get(0), SUBPARTITION_VALUES.get(1), SUBPARTITION_VALUES.get(2), SUBPARTITION_VALUES.get(3), DATA_VERSION,
                ATTRIBUTE_NAME_1_MIXED_CASE);

        // Verify the external calls.
        verify(businessObjectDataAttributeService).getBusinessObjectDataAttribute(businessObjectDataAttributeKey);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(businessObjectDataAttribute, result);
    }

    @Test
    public void testGetBusinessObjectDataAttributesSubPartitionValuesCount0()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION);

        // Create a business object data attribute keys.
        BusinessObjectDataAttributeKeys businessObjectDataAttributeKeys = new BusinessObjectDataAttributeKeys(Arrays.asList(
            new BusinessObjectDataAttributeKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE)));

        // Mock the external calls.
        when(businessObjectDataAttributeService.getBusinessObjectDataAttributes(businessObjectDataKey)).thenReturn(businessObjectDataAttributeKeys);

        // Call the method under test.
        BusinessObjectDataAttributeKeys result = businessObjectDataAttributeRestController
            .getBusinessObjectDataAttributes(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                DATA_VERSION);

        // Verify the external calls.
        verify(businessObjectDataAttributeService).getBusinessObjectDataAttributes(businessObjectDataKey);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(businessObjectDataAttributeKeys, result);
    }

    @Test
    public void testGetBusinessObjectDataAttributesSubPartitionValuesCount1()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                Arrays.asList(SUBPARTITION_VALUES.get(0)), DATA_VERSION);

        // Create a business object data attribute keys.
        BusinessObjectDataAttributeKeys businessObjectDataAttributeKeys = new BusinessObjectDataAttributeKeys(Arrays.asList(
            new BusinessObjectDataAttributeKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE)));

        // Mock the external calls.
        when(businessObjectDataAttributeService.getBusinessObjectDataAttributes(businessObjectDataKey)).thenReturn(businessObjectDataAttributeKeys);

        // Call the method under test.
        BusinessObjectDataAttributeKeys result = businessObjectDataAttributeRestController
            .getBusinessObjectDataAttributes(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES.get(0), DATA_VERSION);

        // Verify the external calls.
        verify(businessObjectDataAttributeService).getBusinessObjectDataAttributes(businessObjectDataKey);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(businessObjectDataAttributeKeys, result);
    }

    @Test
    public void testGetBusinessObjectDataAttributesSubPartitionValuesCount2()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                Arrays.asList(SUBPARTITION_VALUES.get(0), SUBPARTITION_VALUES.get(1)), DATA_VERSION);

        // Create a business object data attribute keys.
        BusinessObjectDataAttributeKeys businessObjectDataAttributeKeys = new BusinessObjectDataAttributeKeys(Arrays.asList(
            new BusinessObjectDataAttributeKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE)));

        // Mock the external calls.
        when(businessObjectDataAttributeService.getBusinessObjectDataAttributes(businessObjectDataKey)).thenReturn(businessObjectDataAttributeKeys);

        // Call the method under test.
        BusinessObjectDataAttributeKeys result = businessObjectDataAttributeRestController
            .getBusinessObjectDataAttributes(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES.get(0), SUBPARTITION_VALUES.get(1), DATA_VERSION);

        // Verify the external calls.
        verify(businessObjectDataAttributeService).getBusinessObjectDataAttributes(businessObjectDataKey);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(businessObjectDataAttributeKeys, result);
    }

    @Test
    public void testGetBusinessObjectDataAttributesSubPartitionValuesCount3()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                Arrays.asList(SUBPARTITION_VALUES.get(0), SUBPARTITION_VALUES.get(1), SUBPARTITION_VALUES.get(2)), DATA_VERSION);

        // Create a business object data attribute keys.
        BusinessObjectDataAttributeKeys businessObjectDataAttributeKeys = new BusinessObjectDataAttributeKeys(Arrays.asList(
            new BusinessObjectDataAttributeKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE)));

        // Mock the external calls.
        when(businessObjectDataAttributeService.getBusinessObjectDataAttributes(businessObjectDataKey)).thenReturn(businessObjectDataAttributeKeys);

        // Call the method under test.
        BusinessObjectDataAttributeKeys result = businessObjectDataAttributeRestController
            .getBusinessObjectDataAttributes(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES.get(0), SUBPARTITION_VALUES.get(1), SUBPARTITION_VALUES.get(2), DATA_VERSION);

        // Verify the external calls.
        verify(businessObjectDataAttributeService).getBusinessObjectDataAttributes(businessObjectDataKey);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(businessObjectDataAttributeKeys, result);
    }

    @Test
    public void testGetBusinessObjectDataAttributesSubPartitionValuesCount4()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                Arrays.asList(SUBPARTITION_VALUES.get(0), SUBPARTITION_VALUES.get(1), SUBPARTITION_VALUES.get(2), SUBPARTITION_VALUES.get(3)), DATA_VERSION);

        // Create a business object data attribute keys.
        BusinessObjectDataAttributeKeys businessObjectDataAttributeKeys = new BusinessObjectDataAttributeKeys(Arrays.asList(
            new BusinessObjectDataAttributeKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE)));

        // Mock the external calls.
        when(businessObjectDataAttributeService.getBusinessObjectDataAttributes(businessObjectDataKey)).thenReturn(businessObjectDataAttributeKeys);

        // Call the method under test.
        BusinessObjectDataAttributeKeys result = businessObjectDataAttributeRestController
            .getBusinessObjectDataAttributes(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES.get(0), SUBPARTITION_VALUES.get(1), SUBPARTITION_VALUES.get(2), SUBPARTITION_VALUES.get(3), DATA_VERSION);

        // Verify the external calls.
        verify(businessObjectDataAttributeService).getBusinessObjectDataAttributes(businessObjectDataKey);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(businessObjectDataAttributeKeys, result);
    }

    @Test
    public void testUpdateBusinessObjectDataAttributeSubPartitionValuesCount0()
    {
        // Create a business object data update request.
        BusinessObjectDataAttributeUpdateRequest businessObjectDataAttributeUpdateRequest = new BusinessObjectDataAttributeUpdateRequest(ATTRIBUTE_VALUE_1);

        // Create a business object data attribute key.
        BusinessObjectDataAttributeKey businessObjectDataAttributeKey =
            new BusinessObjectDataAttributeKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE);

        // Create a business object data attribute.
        BusinessObjectDataAttribute businessObjectDataAttribute = new BusinessObjectDataAttribute(ID, businessObjectDataAttributeKey, ATTRIBUTE_VALUE_1);

        // Mock the external calls.
        when(businessObjectDataAttributeService.updateBusinessObjectDataAttribute(businessObjectDataAttributeKey, businessObjectDataAttributeUpdateRequest))
            .thenReturn(businessObjectDataAttribute);

        // Call the method under test.
        BusinessObjectDataAttribute result = businessObjectDataAttributeRestController
            .updateBusinessObjectDataAttribute(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, businessObjectDataAttributeUpdateRequest);

        // Verify the external calls.
        verify(businessObjectDataAttributeService).updateBusinessObjectDataAttribute(businessObjectDataAttributeKey, businessObjectDataAttributeUpdateRequest);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(businessObjectDataAttribute, result);
    }

    @Test
    public void testUpdateBusinessObjectDataAttributeSubPartitionValuesCount1()
    {
        // Create a business object data update request.
        BusinessObjectDataAttributeUpdateRequest businessObjectDataAttributeUpdateRequest = new BusinessObjectDataAttributeUpdateRequest(ATTRIBUTE_VALUE_1);

        // Create a business object data attribute key.
        BusinessObjectDataAttributeKey businessObjectDataAttributeKey =
            new BusinessObjectDataAttributeKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                Arrays.asList(SUBPARTITION_VALUES.get(0)), DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE);

        // Create a business object data attribute.
        BusinessObjectDataAttribute businessObjectDataAttribute = new BusinessObjectDataAttribute(ID, businessObjectDataAttributeKey, ATTRIBUTE_VALUE_1);

        // Mock the external calls.
        when(businessObjectDataAttributeService.updateBusinessObjectDataAttribute(businessObjectDataAttributeKey, businessObjectDataAttributeUpdateRequest))
            .thenReturn(businessObjectDataAttribute);

        // Call the method under test.
        BusinessObjectDataAttribute result = businessObjectDataAttributeRestController
            .updateBusinessObjectDataAttribute(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES.get(0), DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, businessObjectDataAttributeUpdateRequest);

        // Verify the external calls.
        verify(businessObjectDataAttributeService).updateBusinessObjectDataAttribute(businessObjectDataAttributeKey, businessObjectDataAttributeUpdateRequest);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(businessObjectDataAttribute, result);
    }

    @Test
    public void testUpdateBusinessObjectDataAttributeSubPartitionValuesCount2()
    {
        // Create a business object data update request.
        BusinessObjectDataAttributeUpdateRequest businessObjectDataAttributeUpdateRequest = new BusinessObjectDataAttributeUpdateRequest(ATTRIBUTE_VALUE_1);

        // Create a business object data attribute key.
        BusinessObjectDataAttributeKey businessObjectDataAttributeKey =
            new BusinessObjectDataAttributeKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                Arrays.asList(SUBPARTITION_VALUES.get(0), SUBPARTITION_VALUES.get(1)), DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE);

        // Create a business object data attribute.
        BusinessObjectDataAttribute businessObjectDataAttribute = new BusinessObjectDataAttribute(ID, businessObjectDataAttributeKey, ATTRIBUTE_VALUE_1);

        // Mock the external calls.
        when(businessObjectDataAttributeService.updateBusinessObjectDataAttribute(businessObjectDataAttributeKey, businessObjectDataAttributeUpdateRequest))
            .thenReturn(businessObjectDataAttribute);

        // Call the method under test.
        BusinessObjectDataAttribute result = businessObjectDataAttributeRestController
            .updateBusinessObjectDataAttribute(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES.get(0), SUBPARTITION_VALUES.get(1), DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, businessObjectDataAttributeUpdateRequest);

        // Verify the external calls.
        verify(businessObjectDataAttributeService).updateBusinessObjectDataAttribute(businessObjectDataAttributeKey, businessObjectDataAttributeUpdateRequest);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(businessObjectDataAttribute, result);
    }

    @Test
    public void testUpdateBusinessObjectDataAttributeSubPartitionValuesCount3()
    {
        // Create a business object data update request.
        BusinessObjectDataAttributeUpdateRequest businessObjectDataAttributeUpdateRequest = new BusinessObjectDataAttributeUpdateRequest(ATTRIBUTE_VALUE_1);

        // Create a business object data attribute key.
        BusinessObjectDataAttributeKey businessObjectDataAttributeKey =
            new BusinessObjectDataAttributeKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                Arrays.asList(SUBPARTITION_VALUES.get(0), SUBPARTITION_VALUES.get(1), SUBPARTITION_VALUES.get(2)), DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE);

        // Create a business object data attribute.
        BusinessObjectDataAttribute businessObjectDataAttribute = new BusinessObjectDataAttribute(ID, businessObjectDataAttributeKey, ATTRIBUTE_VALUE_1);

        // Mock the external calls.
        when(businessObjectDataAttributeService.updateBusinessObjectDataAttribute(businessObjectDataAttributeKey, businessObjectDataAttributeUpdateRequest))
            .thenReturn(businessObjectDataAttribute);

        // Call the method under test.
        BusinessObjectDataAttribute result = businessObjectDataAttributeRestController
            .updateBusinessObjectDataAttribute(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES.get(0), SUBPARTITION_VALUES.get(1), SUBPARTITION_VALUES.get(2), DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE,
                businessObjectDataAttributeUpdateRequest);

        // Verify the external calls.
        verify(businessObjectDataAttributeService).updateBusinessObjectDataAttribute(businessObjectDataAttributeKey, businessObjectDataAttributeUpdateRequest);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(businessObjectDataAttribute, result);
    }

    @Test
    public void testUpdateBusinessObjectDataAttributeSubPartitionValuesCount4()
    {
        // Create a business object data update request.
        BusinessObjectDataAttributeUpdateRequest businessObjectDataAttributeUpdateRequest = new BusinessObjectDataAttributeUpdateRequest(ATTRIBUTE_VALUE_1);

        // Create a business object data attribute key.
        BusinessObjectDataAttributeKey businessObjectDataAttributeKey =
            new BusinessObjectDataAttributeKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                Arrays.asList(SUBPARTITION_VALUES.get(0), SUBPARTITION_VALUES.get(1), SUBPARTITION_VALUES.get(2), SUBPARTITION_VALUES.get(3)), DATA_VERSION,
                ATTRIBUTE_NAME_1_MIXED_CASE);

        // Create a business object data attribute.
        BusinessObjectDataAttribute businessObjectDataAttribute = new BusinessObjectDataAttribute(ID, businessObjectDataAttributeKey, ATTRIBUTE_VALUE_1);

        // Mock the external calls.
        when(businessObjectDataAttributeService.updateBusinessObjectDataAttribute(businessObjectDataAttributeKey, businessObjectDataAttributeUpdateRequest))
            .thenReturn(businessObjectDataAttribute);

        // Call the method under test.
        BusinessObjectDataAttribute result = businessObjectDataAttributeRestController
            .updateBusinessObjectDataAttribute(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES.get(0), SUBPARTITION_VALUES.get(1), SUBPARTITION_VALUES.get(2), SUBPARTITION_VALUES.get(3), DATA_VERSION,
                ATTRIBUTE_NAME_1_MIXED_CASE, businessObjectDataAttributeUpdateRequest);

        // Verify the external calls.
        verify(businessObjectDataAttributeService).updateBusinessObjectDataAttribute(businessObjectDataAttributeKey, businessObjectDataAttributeUpdateRequest);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(businessObjectDataAttribute, result);
    }

    /**
     * Checks if any of the mocks has any interaction.
     */
    private void verifyNoMoreInteractionsHelper()
    {
        verifyNoMoreInteractions(businessObjectDataAttributeService);
    }
}
