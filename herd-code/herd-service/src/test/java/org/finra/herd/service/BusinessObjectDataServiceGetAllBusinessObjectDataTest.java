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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.BusinessObjectDataDao;
import org.finra.herd.dao.StorageUnitDao;
import org.finra.herd.dao.helper.JsonHelper;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectDataKeys;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.service.helper.BusinessObjectDataDaoHelper;
import org.finra.herd.service.helper.BusinessObjectDataHelper;
import org.finra.herd.service.helper.BusinessObjectDataInvalidateUnregisteredHelper;
import org.finra.herd.service.helper.BusinessObjectDataRetryStoragePolicyTransitionHelper;
import org.finra.herd.service.helper.BusinessObjectDataSearchHelper;
import org.finra.herd.service.helper.BusinessObjectDataStatusDaoHelper;
import org.finra.herd.service.helper.BusinessObjectDefinitionDaoHelper;
import org.finra.herd.service.helper.BusinessObjectDefinitionHelper;
import org.finra.herd.service.helper.BusinessObjectFormatDaoHelper;
import org.finra.herd.service.helper.BusinessObjectFormatHelper;
import org.finra.herd.service.helper.CustomDdlDaoHelper;
import org.finra.herd.service.helper.DdlGeneratorFactory;
import org.finra.herd.service.helper.S3KeyPrefixHelper;
import org.finra.herd.service.helper.StorageDaoHelper;
import org.finra.herd.service.helper.StorageHelper;
import org.finra.herd.service.helper.StorageUnitHelper;
import org.finra.herd.service.impl.BusinessObjectDataServiceImpl;

/**
 * This class tests get all business object data functionality within the business object data service.
 */
public class BusinessObjectDataServiceGetAllBusinessObjectDataTest extends AbstractServiceTest
{
    @Mock
    private BusinessObjectDataDao businessObjectDataDao;

    @Mock
    private BusinessObjectDataDaoHelper businessObjectDataDaoHelper;

    @Mock
    private BusinessObjectDataHelper businessObjectDataHelper;

    @Mock
    private BusinessObjectDataInitiateRestoreHelperService businessObjectDataInitiateRestoreHelperService;

    @Mock
    private BusinessObjectDataInvalidateUnregisteredHelper businessObjectDataInvalidateUnregisteredHelper;

    @Mock
    private BusinessObjectDataRetryStoragePolicyTransitionHelper businessObjectDataRetryStoragePolicyTransitionHelper;

    @Mock
    private BusinessObjectDataSearchHelper businessObjectDataSearchHelper;

    @InjectMocks
    private BusinessObjectDataServiceImpl businessObjectDataService;

    @Mock
    private BusinessObjectDataStatusDaoHelper businessObjectDataStatusDaoHelper;

    @Mock
    private BusinessObjectDefinitionDaoHelper businessObjectDefinitionDaoHelper;

    @Mock
    private BusinessObjectDefinitionHelper businessObjectDefinitionHelper;

    @Mock
    private BusinessObjectFormatDaoHelper businessObjectFormatDaoHelper;

    @Mock
    private BusinessObjectFormatHelper businessObjectFormatHelper;

    @Mock
    private ConfigurationHelper configurationHelper;

    @Mock
    private CustomDdlDaoHelper customDdlDaoHelper;

    @Mock
    private DdlGeneratorFactory ddlGeneratorFactory;

    @Mock
    private JsonHelper jsonHelper;

    @Mock
    private NotificationEventService notificationEventService;

    @Mock
    private S3KeyPrefixHelper s3KeyPrefixHelper;

    @Mock
    private S3Service s3Service;

    @Mock
    private StorageDaoHelper storageDaoHelper;

    @Mock
    private StorageHelper storageHelper;

    @Mock
    private StorageUnitDao storageUnitDao;

    @Mock
    private StorageUnitHelper storageUnitHelper;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testGetAllBusinessObjectDataByBusinessObjectDefinition()
    {
        // Create a business object definition key.
        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME);

        // Create a business object definition entity.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity =
            businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(businessObjectDefinitionKey, DATA_PROVIDER_NAME, BDEF_DESCRIPTION);

        // Create a list of business object data keys.
        List<BusinessObjectDataKey> businessObjectDataKeys = Arrays.asList(
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION));

        // Mock the external calls.
        when(businessObjectDefinitionDaoHelper.getBusinessObjectDefinitionEntity(businessObjectDefinitionKey)).thenReturn(businessObjectDefinitionEntity);
        when(configurationHelper.getProperty(ConfigurationValue.BUSINESS_OBJECT_DATA_SEARCH_MAX_RESULTS, Integer.class)).thenReturn(MAX_RESULTS_1);
        when(businessObjectDataDao.getBusinessObjectDataByBusinessObjectDefinition(businessObjectDefinitionEntity, MAX_RESULTS_1))
            .thenReturn(businessObjectDataKeys);

        // Call the method being tested.
        BusinessObjectDataKeys response = businessObjectDataService.getAllBusinessObjectDataByBusinessObjectDefinition(businessObjectDefinitionKey);

        // Verify the external calls.
        verify(businessObjectDefinitionHelper).validateBusinessObjectDefinitionKey(businessObjectDefinitionKey);
        verify(businessObjectDefinitionDaoHelper).getBusinessObjectDefinitionEntity(businessObjectDefinitionKey);
        verify(configurationHelper).getProperty(ConfigurationValue.BUSINESS_OBJECT_DATA_SEARCH_MAX_RESULTS, Integer.class);
        verify(businessObjectDataDao).getBusinessObjectDataByBusinessObjectDefinition(businessObjectDefinitionEntity, MAX_RESULTS_1);
        verifyNoMoreInteractionsHelper();

        // Validate the returned object.
        assertEquals(new BusinessObjectDataKeys(businessObjectDataKeys), response);
    }

    @Test
    public void testGetAllBusinessObjectDataByBusinessObjectFormat()
    {
        // Create a business object definition key.
        BusinessObjectFormatKey businessObjectFormatKey =
            new BusinessObjectFormatKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION);

        // Create a business object format entity.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(businessObjectFormatKey, FORMAT_DESCRIPTION, FORMAT_DOCUMENT_SCHEMA, NO_LATEST_VERSION_FLAG_SET, PARTITION_KEY);

        // Create a list of business object data keys.
        List<BusinessObjectDataKey> businessObjectDataKeys = Arrays.asList(
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION));

        // Mock the external calls.
        when(businessObjectFormatDaoHelper.getBusinessObjectFormatEntity(businessObjectFormatKey)).thenReturn(businessObjectFormatEntity);
        when(configurationHelper.getProperty(ConfigurationValue.BUSINESS_OBJECT_DATA_SEARCH_MAX_RESULTS, Integer.class)).thenReturn(MAX_RESULTS_1);
        when(businessObjectDataDao.getBusinessObjectDataByBusinessObjectFormat(businessObjectFormatEntity, MAX_RESULTS_1)).thenReturn(businessObjectDataKeys);

        // Call the method being tested.
        BusinessObjectDataKeys response = businessObjectDataService.getAllBusinessObjectDataByBusinessObjectFormat(businessObjectFormatKey);

        // Verify the external calls.
        verify(businessObjectFormatHelper).validateBusinessObjectFormatKey(businessObjectFormatKey, true);
        verify(businessObjectFormatDaoHelper).getBusinessObjectFormatEntity(businessObjectFormatKey);
        verify(configurationHelper).getProperty(ConfigurationValue.BUSINESS_OBJECT_DATA_SEARCH_MAX_RESULTS, Integer.class);
        verify(businessObjectDataDao).getBusinessObjectDataByBusinessObjectFormat(businessObjectFormatEntity, MAX_RESULTS_1);
        verifyNoMoreInteractionsHelper();

        // Validate the returned object.
        assertEquals(new BusinessObjectDataKeys(businessObjectDataKeys), response);
    }

    /**
     * Checks if any of the mocks has any unverified interaction.
     */
    private void verifyNoMoreInteractionsHelper()
    {
        verifyNoMoreInteractions(businessObjectDataDao, businessObjectDataDaoHelper, businessObjectDataHelper, businessObjectDataInitiateRestoreHelperService,
            businessObjectDataInvalidateUnregisteredHelper, businessObjectDataRetryStoragePolicyTransitionHelper, businessObjectDataSearchHelper,
            businessObjectDataStatusDaoHelper, businessObjectDefinitionDaoHelper, businessObjectDefinitionHelper, businessObjectFormatDaoHelper,
            businessObjectFormatHelper, configurationHelper, customDdlDaoHelper, ddlGeneratorFactory, jsonHelper, notificationEventService, s3KeyPrefixHelper,
            s3Service, storageDaoHelper, storageHelper, storageUnitDao, storageUnitHelper);
    }
}
