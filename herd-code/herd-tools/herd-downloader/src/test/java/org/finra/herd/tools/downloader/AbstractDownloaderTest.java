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
package org.finra.herd.tools.downloader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.springframework.beans.factory.annotation.Autowired;

import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.dto.DownloaderInputManifestDto;
import org.finra.herd.model.dto.ManifestFile;
import org.finra.herd.model.dto.RegServerAccessParamsDto;
import org.finra.herd.model.dto.S3FileTransferRequestParamsDto;
import org.finra.herd.model.dto.UploaderInputManifestDto;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.tools.common.databridge.AbstractDataBridgeTest;

/**
 * This is an abstract base class that provides useful methods for downloader test drivers.
 */
public abstract class AbstractDownloaderTest extends AbstractDataBridgeTest
{
    /**
     * Downloader web client.
     */
    @Autowired
    protected DownloaderWebClient downloaderWebClient;

    /**
     * Provide easy access to the DownloaderManifestReader instance for all test methods.
     */
    @Autowired
    protected DownloaderManifestReader downloaderManifestReader;

    /**
     * Provide easy access to the DownloaderController for all test methods.
     */
    @Autowired
    protected DownloaderController downloaderController;

    /**
     * Sets up the test environment.
     */
    @Before
    public void setupEnv() throws IOException
    {
        super.setupEnv();

        // Create and initialize a downloader web client instance.
        RegServerAccessParamsDto regServerAccessParamsDto =
            RegServerAccessParamsDto.builder().regServerHost(WEB_SERVICE_HOSTNAME).regServerPort(WEB_SERVICE_HTTPS_PORT).useSsl(true)
                .username(WEB_SERVICE_HTTPS_USERNAME).password(WEB_SERVICE_HTTPS_PASSWORD).build();
        downloaderWebClient.setRegServerAccessParamsDto(regServerAccessParamsDto);
    }

    /**
     * Returns a downloader input downloaderInputManifestDto instance initialized per hard coded test values.
     *
     * @return the resulting DownloaderInputManifestDto instance
     */
    protected DownloaderInputManifestDto getTestDownloaderInputManifestDto()
    {
        DownloaderInputManifestDto manifest = new DownloaderInputManifestDto();

        manifest.setNamespace(TEST_NAMESPACE);
        manifest.setBusinessObjectDefinitionName(TEST_BUSINESS_OBJECT_DEFINITION);
        manifest.setBusinessObjectFormatUsage(TEST_BUSINESS_OBJECT_FORMAT_USAGE);
        manifest.setBusinessObjectFormatFileType(TEST_BUSINESS_OBJECT_FORMAT_FILE_TYPE);
        manifest.setBusinessObjectFormatVersion(TEST_BUSINESS_OBJECT_FORMAT_VERSION.toString());
        manifest.setPartitionKey(TEST_BUSINESS_OBJECT_FORMAT_PARTITION_KEY);
        manifest.setPartitionValue(TEST_PARTITION_VALUE);
        manifest.setSubPartitionValues(TEST_SUB_PARTITION_VALUES);
        manifest.setBusinessObjectDataVersion(TEST_DATA_VERSION_V0.toString());

        return manifest;
    }

    /**
     * Uploads and registers a version if of the test business object data with the specified data files.
     *
     * @param s3KeyPrefix the destination S3 key prefix that must comply with the S3 naming conventions including the expected data version value
     * @param manifestFiles the test data files to be uploaded to S3 and registered
     */
    protected void uploadAndRegisterTestData(String s3KeyPrefix, List<ManifestFile> manifestFiles) throws Exception
    {
        uploadAndRegisterTestData(s3KeyPrefix, manifestFiles, new ArrayList<String>());
    }

    /**
     * Uploads and registers a version if of the test business object data with the specified data files.
     *
     * @param s3KeyPrefix the destination S3 key prefix that must comply with the S3 naming conventions including the expected data version value
     * @param manifestFiles the test data files to be uploaded to S3 and registered
     * @param directoryPaths the list of directory paths to be created in S3 relative to the S3 key prefix
     */
    protected void uploadAndRegisterTestData(String s3KeyPrefix, List<ManifestFile> manifestFiles, List<String> directoryPaths) throws Exception
    {
        uploadTestDataFilesToS3(s3KeyPrefix, manifestFiles, directoryPaths);
        UploaderInputManifestDto uploaderInputManifestDto = getTestUploaderInputManifestDto();
        uploaderInputManifestDto.setManifestFiles(manifestFiles);
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = getTestS3FileTransferRequestParamsDto();
        s3FileTransferRequestParamsDto.setS3KeyPrefix(s3KeyPrefix + "/");
        BusinessObjectData businessObjectData =
            downloaderWebClient.preRegisterBusinessObjectData(uploaderInputManifestDto, StorageEntity.MANAGED_STORAGE, false);
        BusinessObjectDataKey businessObjectDataKey = businessObjectDataHelper.getBusinessObjectDataKey(businessObjectData);
        downloaderWebClient.addStorageFiles(businessObjectDataKey, uploaderInputManifestDto, s3FileTransferRequestParamsDto, StorageEntity.MANAGED_STORAGE);
        downloaderWebClient.updateBusinessObjectDataStatus(businessObjectDataKey, BusinessObjectDataStatusEntity.VALID);
        // Clean up the local input directory used for the test data files upload.
        FileUtils.cleanDirectory(LOCAL_TEMP_PATH_INPUT.toFile());
    }

    /**
     * Uploads and registers a version if of the test business object data.
     *
     * @param s3KeyPrefix the destination S3 key prefix that must comply with the S3 naming conventions including the expected data version value
     */
    protected void uploadAndRegisterTestData(String s3KeyPrefix) throws Exception
    {
        uploadAndRegisterTestData(s3KeyPrefix, testManifestFiles);
    }
}
