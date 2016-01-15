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
package org.finra.herd.tools.common.databridge;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import org.finra.herd.model.dto.DataBridgeBaseManifestDto;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.tools.common.databridge.DataBridgeController;

/**
 * Tests the Data Bridge Controller.
 */
public class DataBridgeControllerTest
{
    @Test
    public void testAdjustIntegerValueAllCases()
    {
        testAdjustIntegerValue(DataBridgeController.MIN_THREADS - 1, DataBridgeController.MIN_THREADS);
        testAdjustIntegerValue(DataBridgeController.MIN_THREADS, DataBridgeController.MIN_THREADS);
        testAdjustIntegerValue(DataBridgeController.MIN_THREADS + 1, DataBridgeController.MIN_THREADS + 1);
        testAdjustIntegerValue(DataBridgeController.MAX_THREADS, DataBridgeController.MAX_THREADS);
        testAdjustIntegerValue(DataBridgeController.MAX_THREADS + 1, DataBridgeController.MAX_THREADS);
    }

    /**
     * When manifest specifies storage name
     * Assert return storage name
     */
    @Test
    public void testGetStorageNameFromManifest_1()
    {
        DataBridgeController dataBridgeController = new BasicDataBridgeController();
        DataBridgeBaseManifestDto manifest = new DataBridgeBaseManifestDto();
        manifest.setStorageName("test");
        String value = dataBridgeController.getStorageNameFromManifest(manifest);
        assertEquals("test", value);
    }

    /**
     * When manifest specifies null storage name
     * Assert return StorageEntity.MANAGED_STORAGE
     */
    @Test
    public void testGetStorageNameFromManifest_2()
    {
        DataBridgeController dataBridgeController = new BasicDataBridgeController();
        DataBridgeBaseManifestDto manifest = new DataBridgeBaseManifestDto();
        manifest.setStorageName(null);
        String value = dataBridgeController.getStorageNameFromManifest(manifest);
        assertEquals(StorageEntity.MANAGED_STORAGE, value);
    }

    /**
     * When manifest does not specify storage name
     * Assert return StorageEntity.MANAGED_STORAGE
     */
    @Test
    public void testGetStorageNameFromManifest_3()
    {
        DataBridgeController dataBridgeController = new BasicDataBridgeController();
        DataBridgeBaseManifestDto manifest = new DataBridgeBaseManifestDto();
        String value = dataBridgeController.getStorageNameFromManifest(manifest);
        assertEquals(StorageEntity.MANAGED_STORAGE, value);
    }

    /**
     * Tests the adjust integer value with a submitted test value and an expected value. The test will fail if the expected value isn't returned.
     *
     * @param testValue the test value to be passed into the method.
     * @param expectedValue the expected value to be returned from the method.
     */
    private void testAdjustIntegerValue(Integer testValue, Integer expectedValue)
    {
        DataBridgeController dataBridgeController = new BasicDataBridgeController();
        assertEquals(expectedValue, dataBridgeController.adjustIntegerValue(testValue, DataBridgeController.MIN_THREADS, DataBridgeController.MAX_THREADS));
    }

    /**
     * Extends the abstract DataBridgeController class so methods in the base class can be tested.
     */
    class BasicDataBridgeController extends DataBridgeController
    {
    }
}
