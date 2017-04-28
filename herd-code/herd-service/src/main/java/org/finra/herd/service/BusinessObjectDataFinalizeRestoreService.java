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

import java.util.List;

import org.finra.herd.model.dto.StorageUnitAlternateKeyDto;

/**
 * The business object data finalize restore service.
 */
public interface BusinessObjectDataFinalizeRestoreService
{
    /**
     * Finalizes restore of an S3 storage unit.
     *
     * @param storageUnitKey the storage unit key
     */
    public void finalizeRestore(StorageUnitAlternateKeyDto storageUnitKey);

    /**
     * Retrieves the keys for S3 storage units that are currently being restored.
     *
     * @param maxResult the maximum number of results to retrieve
     *
     * @return the list of storage unit keys
     */
    public List<StorageUnitAlternateKeyDto> getS3StorageUnitsToRestore(int maxResult);
}
