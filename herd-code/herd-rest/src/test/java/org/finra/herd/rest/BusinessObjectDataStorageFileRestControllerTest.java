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

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.model.api.xml.BusinessObjectDataStorageFilesCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataStorageFilesCreateResponse;
import org.finra.herd.service.BusinessObjectDataStorageFileService;

/**
 * This class tests CreateBusinessObjectDataStorageFiles functionality within the business object data storage file REST controller.
 */
public class BusinessObjectDataStorageFileRestControllerTest extends AbstractRestTest
{
    @Mock
    private BusinessObjectDataStorageFileService businessObjectDataStorageFileService;

    @InjectMocks
    private BusinessObjectDataStorageFileRestController businessObjectDataStorageFileRestController;

    @Before()
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }
    
    @Test
    public void testCreateBusinessObjectDataStorageFiles()
    {
        BusinessObjectDataStorageFilesCreateResponse response = new BusinessObjectDataStorageFilesCreateResponse();

        BusinessObjectDataStorageFilesCreateRequest request =
            new BusinessObjectDataStorageFilesCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                null, DATA_VERSION, STORAGE_NAME, null,
                NO_DISCOVER_STORAGE_FILES);

        when(businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(request)).thenReturn(response);
        BusinessObjectDataStorageFilesCreateResponse resultResponse = businessObjectDataStorageFileRestController.createBusinessObjectDataStorageFiles(request);

        // Verify the external calls.
        verify(businessObjectDataStorageFileService).createBusinessObjectDataStorageFiles(request);
        verifyNoMoreInteractions(businessObjectDataStorageFileService);
        // Validate the returned object.
        assertEquals(response, resultResponse);
    }
}
