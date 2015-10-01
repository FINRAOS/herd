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
package org.finra.dm.rest;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.finra.dm.model.dto.S3FileTransferRequestParamsDto;
import org.finra.dm.model.jpa.BusinessObjectDataEntity;
import org.finra.dm.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.dm.model.jpa.BusinessObjectFormatEntity;
import org.finra.dm.model.jpa.DataProviderEntity;
import org.finra.dm.model.jpa.FileTypeEntity;
import org.finra.dm.model.jpa.NamespaceEntity;
import org.finra.dm.model.jpa.StorageEntity;
import org.finra.dm.model.jpa.StorageUnitEntity;
import org.finra.dm.model.api.xml.BusinessObjectDataStorageFilesCreateRequest;
import org.finra.dm.model.api.xml.BusinessObjectDataStorageFilesCreateResponse;
import org.finra.dm.model.api.xml.SchemaColumn;
import org.finra.dm.model.api.xml.StorageFile;

/**
 * Tests for {@link BusinessObjectDataStorageFileRestController#createBusinessObjectDataStorageFiles(org.finra.dm.model.api.xml.BusinessObjectDataStorageFilesCreateRequest)}
 */
public class BusinessObjectDataStorageFileRestControllerTest extends AbstractRestTest
{
    private static final String FILE_PATH_1 = "file1";
    private static final String FILE_PATH_2 = "file2";

    private static final String PARTITION_KEY_2 = "pk2_" + Math.random();
    private static final String PARTITION_KEY_3 = "pk3_" + Math.random();
    private static final String PARTITION_KEY_4 = "pk4_" + Math.random();
    private static final String PARTITION_KEY_5 = "pk5_" + Math.random();

    private static final String PARTITION_VALUE_2 = "pv2_" + Math.random();
    private static final String PARTITION_VALUE_3 = "pv3_" + Math.random();
    private static final String PARTITION_VALUE_4 = "pv4_" + Math.random();
    private static final String PARTITION_VALUE_5 = "pv5_" + Math.random();

    private static final List<String> SUB_PARTITION_VALUES = Arrays.asList(PARTITION_VALUE_2, PARTITION_VALUE_3, PARTITION_VALUE_4, PARTITION_VALUE_5);

    private String testS3KeyPrefix =
        getExpectedS3KeyPrefix(NAMESPACE_CD, DATA_PROVIDER_NAME, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_KEY,
            PARTITION_VALUE, null, null, DATA_VERSION);

    /**
     * Initialize the environment. This method is run once before any of the test methods in the class.
     */
    @BeforeClass
    public static void initEnv() throws IOException
    {
        localTempPath = Paths.get(System.getProperty("java.io.tmpdir"), "dm-bod-service-create-test-local-folder");
    }

    /**
     * Sets up the test environment.
     */
    @Before
    public void setupEnv() throws IOException
    {
        // Create local temp directory.
        localTempPath.toFile().mkdir();
    }

    /**
     * Cleans up the local temp directory and S3 test path that we are using.
     */
    @After
    public void cleanEnv() throws IOException
    {
        // Clean up the local directory.
        FileUtils.deleteDirectory(localTempPath.toFile());

        // Clean up the destination S3 folder.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = getTestS3FileTransferRequestParamsDto();
        s3FileTransferRequestParamsDto.setS3KeyPrefix(testS3KeyPrefix);
        s3Dao.deleteDirectory(s3FileTransferRequestParamsDto);
    }

    @Test
    public void testCreateBusinessObjectDataStorageFiles()
    {
        createDataWithSubPartitions();

        BusinessObjectDataStorageFilesCreateRequest request =
            createBusinessObjectDataStorageFilesCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUB_PARTITION_VALUES, DATA_VERSION, STORAGE_NAME, Arrays.asList(createFile(FILE_PATH_2, FILE_SIZE_1_KB, ROW_COUNT_1000)));

        BusinessObjectDataStorageFilesCreateResponse response = businessObjectDataStorageFileRestController.createBusinessObjectDataStorageFiles(request);

        // Validate the returned object.
        validateBusinessObjectDataStorageFilesCreateResponse(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
            SUB_PARTITION_VALUES, DATA_VERSION, STORAGE_NAME, request.getStorageFiles(), response);
    }

    private StorageFile createFile(String filePath, Long size, Long rowCount)
    {
        StorageFile f = new StorageFile();
        f.setFilePath(filePath);
        f.setFileSizeBytes(size);
        f.setRowCount(rowCount);
        return f;
    }

    private void createDataWithSubPartitions()
    {
        NamespaceEntity namespaceEntity = super.createNamespaceEntity(NAMESPACE_CD);
        DataProviderEntity dataProviderEntity = super.createDataProviderEntity(DATA_PROVIDER_NAME);
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity =
            super.createBusinessObjectDefinitionEntity(namespaceEntity, BOD_NAME, dataProviderEntity, null, null, true);
        FileTypeEntity fileTypeEntity = super.createFileTypeEntity(FORMAT_FILE_TYPE_CODE, FORMAT_DESCRIPTION);
        List<SchemaColumn> schemaColumns = new ArrayList<>();
        {
            SchemaColumn schemaColumn = new SchemaColumn();
            schemaColumn.setName(PARTITION_KEY);
            schemaColumn.setType("STRING");
            schemaColumns.add(schemaColumn);
        }
        {
            SchemaColumn schemaColumn = new SchemaColumn();
            schemaColumn.setName(PARTITION_KEY_2);
            schemaColumn.setType("STRING");
            schemaColumns.add(schemaColumn);
        }
        {
            SchemaColumn schemaColumn = new SchemaColumn();
            schemaColumn.setName(PARTITION_KEY_3);
            schemaColumn.setType("STRING");
            schemaColumns.add(schemaColumn);
        }
        {
            SchemaColumn schemaColumn = new SchemaColumn();
            schemaColumn.setName(PARTITION_KEY_4);
            schemaColumn.setType("STRING");
            schemaColumns.add(schemaColumn);
        }
        {
            SchemaColumn schemaColumn = new SchemaColumn();
            schemaColumn.setName(PARTITION_KEY_5);
            schemaColumn.setType("STRING");
            schemaColumns.add(schemaColumn);
        }
        BusinessObjectFormatEntity businessObjectFormatEntity = super
            .createBusinessObjectFormatEntity(businessObjectDefinitionEntity, FORMAT_USAGE_CODE, fileTypeEntity, FORMAT_VERSION, null, true, PARTITION_KEY,
                null, null, null, null, schemaColumns, null);
        BusinessObjectDataEntity businessObjectDataEntity =
            createBusinessObjectDataEntity(businessObjectFormatEntity, PARTITION_VALUE, SUB_PARTITION_VALUES, DATA_VERSION, true, BDATA_STATUS);

        StorageEntity storageEntity = super.createStorageEntity(STORAGE_NAME);
        StorageUnitEntity storageUnitEntity = super.createStorageUnitEntity(storageEntity, businessObjectDataEntity);

        super.createStorageFileEntity(storageUnitEntity, FILE_PATH_1, FILE_SIZE_1_KB, null);
    }
}
