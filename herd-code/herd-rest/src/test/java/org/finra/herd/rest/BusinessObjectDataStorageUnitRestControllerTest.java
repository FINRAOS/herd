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

import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.model.api.xml.BusinessObjectDataStorageUnitCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataStorageUnitCreateResponse;
import org.finra.herd.model.api.xml.BusinessObjectDataStorageUnitKey;
import org.finra.herd.model.api.xml.StorageDirectory;
import org.finra.herd.model.api.xml.StorageFile;
import org.finra.herd.service.BusinessObjectDataStorageUnitService;

/**
 * This class tests various functionality within the business object data storage unit REST controller.
 */
public class BusinessObjectDataStorageUnitRestControllerTest extends AbstractRestTest
{
    @InjectMocks
    private BusinessObjectDataStorageUnitRestController businessObjectDataStorageUnitRestController;

    @Mock
    private BusinessObjectDataStorageUnitService businessObjectDataStorageUnitService;

    @Before()
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCreateBusinessObjectDataStorageUnit()
    {
        // Create a business object data storage key.
        BusinessObjectDataStorageUnitKey businessObjectDataStorageUnitKey =
            new BusinessObjectDataStorageUnitKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, STORAGE_NAME);

        // Create a storage unit directory.
        StorageDirectory storageDirectory = new StorageDirectory(STORAGE_DIRECTORY_PATH);

        // Create a list of storage files.
        List<StorageFile> storageFiles = Collections.singletonList(new StorageFile(FILE_NAME, FILE_SIZE, ROW_COUNT));

        // Create a business object data storage unit request.
        BusinessObjectDataStorageUnitCreateRequest request =
            new BusinessObjectDataStorageUnitCreateRequest(businessObjectDataStorageUnitKey, storageDirectory, storageFiles, DISCOVER_STORAGE_FILES);

        // Create a business object data storage unit response.
        BusinessObjectDataStorageUnitCreateResponse response =
            new BusinessObjectDataStorageUnitCreateResponse(businessObjectDataStorageUnitKey, storageDirectory, storageFiles);

        // Mock the external calls.
        when(businessObjectDataStorageUnitService.createBusinessObjectDataStorageUnit(request)).thenReturn(response);

        // Call the method under test.
        BusinessObjectDataStorageUnitCreateResponse result = businessObjectDataStorageUnitRestController.createBusinessObjectDataStorageUnit(request);

        // Verify the external calls.
        verify(businessObjectDataStorageUnitService).createBusinessObjectDataStorageUnit(request);
        verifyNoMoreInteractions(businessObjectDataStorageUnitService);

        // Validate the results.
        assertEquals(response, result);
    }
}
