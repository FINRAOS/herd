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
package org.finra.herd.service.helper;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.dto.StorageUnitAlternateKeyDto;
import org.finra.herd.service.AbstractServiceTest;

public class StorageUnitHelperTest extends AbstractServiceTest
{
    @Test
    public void testCreateStorageUnitKey()
    {
        // Get a storage unit key.
        StorageUnitAlternateKeyDto resultStorageUnitAlternateKeyDto = storageUnitHelper.createStorageUnitKey(
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION), STORAGE_NAME);

        // Validate the result object.
        assertEquals(resultStorageUnitAlternateKeyDto.getNamespace(), BDEF_NAMESPACE);
        assertEquals(resultStorageUnitAlternateKeyDto.getBusinessObjectDefinitionName(), BDEF_NAME);
        assertEquals(resultStorageUnitAlternateKeyDto.getBusinessObjectFormatUsage(), FORMAT_USAGE_CODE);
        assertEquals(resultStorageUnitAlternateKeyDto.getBusinessObjectFormatFileType(), FORMAT_FILE_TYPE_CODE);
        assertEquals(resultStorageUnitAlternateKeyDto.getBusinessObjectFormatVersion(), FORMAT_VERSION);
        assertEquals(resultStorageUnitAlternateKeyDto.getPartitionValue(), PARTITION_VALUE);
        assertEquals(resultStorageUnitAlternateKeyDto.getSubPartitionValues(), SUBPARTITION_VALUES);
        assertEquals(resultStorageUnitAlternateKeyDto.getBusinessObjectDataVersion(), DATA_VERSION);
        assertEquals(resultStorageUnitAlternateKeyDto.getStorageName(), STORAGE_NAME);
    }
}
