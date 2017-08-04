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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.StoragePlatform;
import org.finra.herd.model.api.xml.StoragePlatforms;
import org.finra.herd.model.jpa.StoragePlatformEntity;
import org.finra.herd.service.StoragePlatformService;

/**
 * This class tests various functionality within the storage platform REST controller.
 */
public class StoragePlatformRestControllerTest extends AbstractRestTest
{
    private static Logger logger = LoggerFactory.getLogger(StoragePlatformRestControllerTest.class);

    @InjectMocks
    private StoragePlatformRestController storagePlatformRestController;

    @Mock
    private StoragePlatformService storagePlatformService;

    @Before()
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testGetStoragePlatform() throws Exception
    {
        StoragePlatform storagePlatform = new StoragePlatform(StoragePlatformEntity.S3);
        when(storagePlatformService.getStoragePlatform(StoragePlatformEntity.S3)).thenReturn(storagePlatform);
        StoragePlatform resultStoragePlatform = storagePlatformRestController.getStoragePlatform(StoragePlatformEntity.S3);
        assertNotNull(storagePlatform);
        assertTrue(StoragePlatformEntity.S3.equals(storagePlatform.getName()));
        logger.info("Storage platform name: " + storagePlatform.getName());
        // Verify the external calls.
        verify(storagePlatformService).getStoragePlatform(StoragePlatformEntity.S3);
        verifyNoMoreInteractions(storagePlatformService);
        // Validate the returned object.
        assertEquals(storagePlatform, resultStoragePlatform);
    }

    @Test(expected = ObjectNotFoundException.class)
    public void testGetStoragePlatformInvalidName() throws Exception
    {
        ObjectNotFoundException exception = new ObjectNotFoundException("not found");
        String platform = "invalid" + getRandomSuffix();
        when(storagePlatformService.getStoragePlatform(platform)).thenThrow(exception);
        storagePlatformRestController.getStoragePlatform(platform);
    }

    @Test
    public void testGetStoragePlatforms() throws Exception
    {
        StoragePlatforms storagePlatforms = new StoragePlatforms(Arrays.asList(new StoragePlatform()));
        when(storagePlatformService.getStoragePlatforms()).thenReturn(storagePlatforms);
        StoragePlatforms resultStoragePlatforms = storagePlatformRestController.getStoragePlatforms();
        assertNotNull(storagePlatforms);
        logger.info("Total number of storage platforms: " + storagePlatforms.getStoragePlatforms().size());
        assertTrue(storagePlatforms.getStoragePlatforms().size() >= 1); // We should have at least 1 row of reference data present (i.e. S3).

        // Verify the external calls.
        verify(storagePlatformService).getStoragePlatforms();
        verifyNoMoreInteractions(storagePlatformService);
        // Validate the returned object.
        assertEquals(storagePlatforms, resultStoragePlatforms);
    }
}
