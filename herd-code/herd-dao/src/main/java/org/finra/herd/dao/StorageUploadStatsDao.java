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
package org.finra.herd.dao;

import org.finra.herd.model.api.xml.StorageBusinessObjectDefinitionDailyUploadStats;
import org.finra.herd.model.api.xml.StorageDailyUploadStats;
import org.finra.herd.model.dto.DateRangeDto;
import org.finra.herd.model.dto.StorageAlternateKeyDto;

public interface StorageUploadStatsDao extends BaseJpaDao
{
    /**
     * Retrieves cumulative daily upload statistics for the storage for the specified upload date range.
     *
     * @param storageAlternateKey the storage alternate key (case-insensitive)
     * @param dateRange the upload date range
     *
     * @return the upload statistics
     */
    public StorageDailyUploadStats getStorageUploadStats(StorageAlternateKeyDto storageAlternateKey, DateRangeDto dateRange);

    /**
     * Retrieves daily upload statistics for the storage by business object definition for the specified upload date range.
     *
     * @param storageAlternateKey the storage alternate key (case-insensitive)
     * @param dateRange the upload date range
     *
     * @return the upload statistics
     */
    public StorageBusinessObjectDefinitionDailyUploadStats getStorageUploadStatsByBusinessObjectDefinition(StorageAlternateKeyDto storageAlternateKey,
        DateRangeDto dateRange);
}
