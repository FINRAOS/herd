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

import static org.junit.Assert.assertEquals;

import java.text.SimpleDateFormat;

import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import org.finra.herd.model.api.xml.StorageBusinessObjectDefinitionDailyUploadStats;
import org.finra.herd.model.api.xml.StorageDailyUploadStats;
import org.finra.herd.model.dto.DateRangeDto;
import org.finra.herd.model.dto.StorageAlternateKeyDto;
import org.finra.herd.model.jpa.StorageFileViewEntity;

public class StorageUploadStatsDaoTest extends AbstractDaoTest
{
    @Autowired
    private StorageUploadStatsDao storageUploadStatsDao;

    private SimpleDateFormat simpleDateFormat;

    @Before
    public void before() throws Exception
    {
        simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");

        {
            StorageFileViewEntity storageFileViewEntity = new StorageFileViewEntity();
            storageFileViewEntity.setStorageFileId(1l);
            storageFileViewEntity.setNamespaceCode(NAMESPACE);
            storageFileViewEntity.setBusinessObjectDefinitionName(BDEF_NAME);
            storageFileViewEntity.setDataProviderCode(DATA_PROVIDER_NAME);
            storageFileViewEntity.setStorageCode(STORAGE_NAME);
            storageFileViewEntity.setFileSizeInBytes(1l);
            storageFileViewEntity.setCreatedDate(simpleDateFormat.parse("1234-12-23"));
            storageUploadStatsDao.save(storageFileViewEntity);
        }
    }

    @Test
    public void testGetStorageUploadStats() throws Exception
    {

        StorageAlternateKeyDto storageAlternateKey = new StorageAlternateKeyDto();
        storageAlternateKey.setStorageName(STORAGE_NAME);
        DateRangeDto dateRange = new DateRangeDto();
        dateRange.setLowerDate(simpleDateFormat.parse("1234-12-22"));
        dateRange.setUpperDate(simpleDateFormat.parse("1234-12-24"));
        StorageDailyUploadStats storageDailyUploadStats = storageUploadStatsDao.getStorageUploadStats(storageAlternateKey, dateRange);
        assertEquals(1, storageDailyUploadStats.getStorageDailyUploadStats().size());
    }

    @Test
    public void testGetStorageUploadStatsByBusinessObjectDefinition() throws Exception
    {
        StorageAlternateKeyDto storageAlternateKey = new StorageAlternateKeyDto();
        storageAlternateKey.setStorageName(STORAGE_NAME);
        DateRangeDto dateRange = new DateRangeDto();
        dateRange.setLowerDate(simpleDateFormat.parse("1234-12-22"));
        dateRange.setUpperDate(simpleDateFormat.parse("1234-12-24"));
        StorageBusinessObjectDefinitionDailyUploadStats storageBusinessObjectDefinitionDailyUploadStats =
            storageUploadStatsDao.getStorageUploadStatsByBusinessObjectDefinition(storageAlternateKey, dateRange);
        assertEquals(1, storageBusinessObjectDefinitionDailyUploadStats.getStorageBusinessObjectDefinitionDailyUploadStats().size());
    }
}
