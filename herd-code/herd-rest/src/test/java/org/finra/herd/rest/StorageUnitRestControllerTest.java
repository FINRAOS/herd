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
import static org.mockito.MockitoAnnotations.initMocks;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;

import org.finra.herd.dao.helper.HerdStringHelper;
import org.finra.herd.model.api.xml.AwsCredential;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.StorageUnitDownloadCredential;
import org.finra.herd.model.api.xml.StorageUnitUploadCredential;
import org.finra.herd.service.StorageUnitService;

public class StorageUnitRestControllerTest extends AbstractRestTest
{
    @Mock
    private HerdStringHelper herdStringHelper;

    @InjectMocks
    private StorageUnitRestController storageUnitRestController;

    @Mock
    private StorageUnitService storageUnitService;

    @Before
    public void before()
    {
        initMocks(this);
    }

    @Test
    public void getStorageUnitDownloadCredential()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a delimited list of sub-partition values.
        String delimitedSubPartitionValues = String.join("|", SUBPARTITION_VALUES);

        // Create a storage unit download credential.
        StorageUnitDownloadCredential storageUnitDownloadCredential = new StorageUnitDownloadCredential(
            new AwsCredential(AWS_ASSUMED_ROLE_ACCESS_KEY, AWS_ASSUMED_ROLE_SECRET_KEY, AWS_ASSUMED_ROLE_SESSION_TOKEN,
                AWS_ASSUMED_ROLE_SESSION_EXPIRATION_TIME));

        // Mock the external calls.
        when(herdStringHelper.splitStringWithDefaultDelimiterEscaped(delimitedSubPartitionValues)).thenReturn(SUBPARTITION_VALUES);
        when(storageUnitService.getStorageUnitDownloadCredential(businessObjectDataKey, STORAGE_NAME)).thenReturn(storageUnitDownloadCredential);

        // Call the method under test.
        StorageUnitDownloadCredential result = storageUnitRestController
            .getStorageUnitDownloadCredential(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                DATA_VERSION, STORAGE_NAME, delimitedSubPartitionValues);

        // Verify the external calls.
        verify(herdStringHelper).splitStringWithDefaultDelimiterEscaped(delimitedSubPartitionValues);
        verify(storageUnitService).getStorageUnitDownloadCredential(businessObjectDataKey, STORAGE_NAME);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(storageUnitDownloadCredential, result);
    }

    @Test
    public void getStorageUnitUploadCredential()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a delimited list of sub-partition values.
        String delimitedSubPartitionValues = String.join("|", SUBPARTITION_VALUES);

        // Create a storage unit upload credential.
        StorageUnitUploadCredential storageUnitUploadCredential = new StorageUnitUploadCredential(
            new AwsCredential(AWS_ASSUMED_ROLE_ACCESS_KEY, AWS_ASSUMED_ROLE_SECRET_KEY, AWS_ASSUMED_ROLE_SESSION_TOKEN,
                AWS_ASSUMED_ROLE_SESSION_EXPIRATION_TIME), AWS_KMS_KEY_ID);

        // Mock the external calls.
        when(herdStringHelper.splitStringWithDefaultDelimiterEscaped(delimitedSubPartitionValues)).thenReturn(SUBPARTITION_VALUES);
        when(storageUnitService.getStorageUnitUploadCredential(businessObjectDataKey, null, STORAGE_NAME)).thenReturn(storageUnitUploadCredential);

        // Call the method under test.
        StorageUnitUploadCredential result = storageUnitRestController
            .getStorageUnitUploadCredential(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, DATA_VERSION,
                STORAGE_NAME, delimitedSubPartitionValues);

        // Verify the external calls.
        verify(herdStringHelper).splitStringWithDefaultDelimiterEscaped(delimitedSubPartitionValues);
        verify(storageUnitService).getStorageUnitUploadCredential(businessObjectDataKey, null, STORAGE_NAME);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(storageUnitUploadCredential, result);
    }

    /**
     * Checks if any of the mocks has any interaction.
     */
    private void verifyNoMoreInteractionsHelper()
    {
        verifyNoMoreInteractions(herdStringHelper, storageUnitService);
    }
}
