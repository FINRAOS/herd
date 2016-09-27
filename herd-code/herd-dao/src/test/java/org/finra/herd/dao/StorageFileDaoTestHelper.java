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

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.model.api.xml.SchemaColumn;
import org.finra.herd.model.jpa.StorageFileEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;

@Component
public class StorageFileDaoTestHelper
{
    @Autowired
    private StorageFileDao storageFileDao;

    /**
     * Creates and persists storage file entities per specified parameters.
     *
     * @param storageUnitEntity the storage unit entity
     * @param s3KeyPrefix the S3 key prefix
     * @param partitionColumns the list of partition columns
     * @param subPartitionValues the list of sub-partition values
     * @param replaceUnderscoresWithHyphens specifies to replace UnderscoresWithHyphens when buidling a path
     */
    public void createStorageFileEntities(StorageUnitEntity storageUnitEntity, String s3KeyPrefix, List<SchemaColumn> partitionColumns,
        List<String> subPartitionValues, boolean replaceUnderscoresWithHyphens)
    {
        int discoverableSubPartitionsCount = partitionColumns != null ? partitionColumns.size() - subPartitionValues.size() - 1 : 0;
        int storageFilesCount = (int) Math.pow(2, discoverableSubPartitionsCount);

        for (int i = 0; i < storageFilesCount; i++)
        {
            // Build a relative sub-directory path.
            StringBuilder subDirectory = new StringBuilder();
            String binaryString = StringUtils.leftPad(Integer.toBinaryString(i), discoverableSubPartitionsCount, "0");
            for (int j = 0; j < discoverableSubPartitionsCount; j++)
            {
                String subpartitionKey = partitionColumns.get(j + subPartitionValues.size() + 1).getName().toLowerCase();
                if (replaceUnderscoresWithHyphens)
                {
                    subpartitionKey = subpartitionKey.replace("_", "-");
                }
                subDirectory.append(String.format("/%s=%s", subpartitionKey, binaryString.substring(j, j + 1)));
            }
            // Create a storage file entity.
            createStorageFileEntity(storageUnitEntity, String.format("%s%s/data.dat", s3KeyPrefix, subDirectory.toString()), AbstractDaoTest.FILE_SIZE_1_KB,
                AbstractDaoTest.ROW_COUNT_1000);
        }
    }

    /**
     * Creates and persists a new storage file entity.
     *
     * @param storageUnitEntity the storage unit entity
     * @param filePath the file path
     * @param fileSizeInBytes the size of the file in bytes
     * @param rowCount the count of rows in the file
     *
     * @return the newly created storage file entity.
     */
    public StorageFileEntity createStorageFileEntity(StorageUnitEntity storageUnitEntity, String filePath, Long fileSizeInBytes, Long rowCount)
    {
        StorageFileEntity storageFileEntity = new StorageFileEntity();
        storageFileEntity.setStorageUnit(storageUnitEntity);
        storageFileEntity.setPath(filePath);
        storageFileEntity.setFileSizeBytes(fileSizeInBytes);
        storageFileEntity.setRowCount(rowCount);
        return storageFileDao.saveAndRefresh(storageFileEntity);
    }
}
