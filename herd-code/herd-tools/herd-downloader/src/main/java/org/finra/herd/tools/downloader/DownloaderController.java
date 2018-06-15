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

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import javax.xml.bind.JAXBException;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import org.finra.herd.core.HerdFileUtils;
import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.model.api.xml.Attribute;
import org.finra.herd.model.api.xml.AwsCredential;
import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.S3KeyPrefixInformation;
import org.finra.herd.model.api.xml.Storage;
import org.finra.herd.model.api.xml.StorageFile;
import org.finra.herd.model.api.xml.StorageUnit;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.DownloaderInputManifestDto;
import org.finra.herd.model.dto.DownloaderOutputManifestDto;
import org.finra.herd.model.dto.ManifestFile;
import org.finra.herd.model.dto.RegServerAccessParamsDto;
import org.finra.herd.model.dto.S3FileTransferRequestParamsDto;
import org.finra.herd.service.helper.BusinessObjectDataHelper;
import org.finra.herd.service.helper.StorageFileHelper;
import org.finra.herd.service.helper.StorageHelper;
import org.finra.herd.tools.common.databridge.AutoRefreshCredentialProvider;
import org.finra.herd.tools.common.databridge.DataBridgeController;

/**
 * Executes the DownloaderApp workflow.
 */
@Component
public class DownloaderController extends DataBridgeController
{
    private static final Logger LOGGER = LoggerFactory.getLogger(DownloaderController.class);

    @Autowired
    private BusinessObjectDataHelper businessObjectDataHelper;

    @Autowired
    private ConfigurationHelper configurationHelper;

    @Autowired
    private DownloaderManifestReader manifestReader;

    @Autowired
    private DownloaderManifestWriter manifestWriter;

    @Autowired
    private DownloaderWebClient downloaderWebClient;

    @Autowired
    private StorageFileHelper storageFileHelper;

    @Autowired
    private StorageHelper storageHelper;

    /**
     * The downloader output manifest file name.
     */
    private static final String OUTPUT_MANIFEST_FILE_NAME = "manifest.json";

    /**
     * Executes the downloader workflow.
     *
     * @param regServerAccessParamsDto the DTO for the parameters required to communicate with the herd registration server
     * @param manifestPath the local path to the manifest file
     * @param s3FileTransferRequestParamsDto the S3 file transfer DTO request parameters
     *
     * @throws InterruptedException if the upload thread was interrupted
     * @throws JAXBException if a JAXB error was encountered
     * @throws IOException if an I/O error was encountered
     * @throws URISyntaxException if a URI syntax error was encountered
     * @throws KeyStoreException if a key store exception occurs
     * @throws NoSuchAlgorithmException if a no such algorithm exception occurs
     * @throws KeyManagementException if key management exception
     */
    @SuppressFBWarnings(value = {"BC_UNCONFIRMED_CAST_OF_RETURN_VALUE", "NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE"}, justification =
        "manifestReader.readJsonManifest will always return an DownloaderInputManifestDto object. targetLocalDirectory.list().length will not" +
            " return a NullPointerException.")
    public void performDownload(RegServerAccessParamsDto regServerAccessParamsDto, File manifestPath,
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto)
        throws InterruptedException, JAXBException, IOException, URISyntaxException, NoSuchAlgorithmException, KeyStoreException, KeyManagementException
    {
        boolean cleanUpTargetLocalDirectoryOnFailure = false;
        File targetLocalDirectory = null;

        try
        {
            // Process manifest file.
            DownloaderInputManifestDto manifest = manifestReader.readJsonManifest(manifestPath);

            String storageName = getStorageNameFromManifest(manifest);

            // Get business object data from the herd registration server.
            downloaderWebClient.setRegServerAccessParamsDto(regServerAccessParamsDto);
            BusinessObjectData businessObjectData = downloaderWebClient.getBusinessObjectData(manifest);

            manifest.setBusinessObjectDataVersion(String.valueOf(businessObjectData.getVersion()));
            manifest.setBusinessObjectFormatVersion(String.valueOf(businessObjectData.getBusinessObjectFormatVersion()));
            s3FileTransferRequestParamsDto.getAdditionalAwsCredentialsProviders().add(new AutoRefreshCredentialProvider()
            {
                @Override
                public AwsCredential getNewAwsCredential() throws Exception
                {
                    return downloaderWebClient.getStorageUnitDownloadCredential(manifest, storageName).getAwsCredential();
                }
            });

            // Get a storage unit that belongs to the S3 storage.
            StorageUnit storageUnit = businessObjectDataHelper.getStorageUnitByStorageName(businessObjectData, storageName);

            // Get the expected S3 key prefix and S3 bucket name.
            S3KeyPrefixInformation s3KeyPrefixInformation = downloaderWebClient.getS3KeyPrefix(businessObjectData);

            // Check if the target folder (local directory + S3 key prefix) exists and try to create it if it does not.
            targetLocalDirectory = Paths.get(s3FileTransferRequestParamsDto.getLocalPath(), s3KeyPrefixInformation.getS3KeyPrefix()).toFile();
            if (!targetLocalDirectory.isDirectory())
            {
                // Create the local directory including any necessary but nonexistent parent directories.
                if (!targetLocalDirectory.mkdirs())
                {
                    throw new IllegalArgumentException(String.format("Failed to create target local directory \"%s\".", targetLocalDirectory.getPath()));
                }
            }
            else
            {
                // Check if the target local directory is empty.
                if (targetLocalDirectory.list().length > 0)
                {
                    throw new IllegalArgumentException(String.format("The target local directory \"%s\" is not empty.", targetLocalDirectory.getPath()));
                }
            }

            // Get S3 bucket information.
            Storage storage = downloaderWebClient.getStorage(storageName);

            // Get S3 bucket name.  Please note that since this value is required we pass a "true" flag.
            String s3BucketName =
                storageHelper.getStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), storage, true);

            // Get the list of S3 files matching the expected S3 key prefix.
            s3FileTransferRequestParamsDto.setS3BucketName(s3BucketName);
            // Since the S3 key prefix represents a directory, we add a trailing '/' character to it.
            s3FileTransferRequestParamsDto.setS3KeyPrefix(s3KeyPrefixInformation.getS3KeyPrefix() + "/");
            // When listing S3 files, we ignore 0 byte objects that represent S3 directories.
            List<String> actualS3Files = storageFileHelper.getFilePathsFromS3ObjectSummaries(s3Service.listDirectory(s3FileTransferRequestParamsDto, true));

            // Validate S3 files before we start the download.
            storageFileHelper.validateStorageUnitS3Files(storageUnit, actualS3Files, s3KeyPrefixInformation.getS3KeyPrefix());

            // Special handling for the maxThreads command line option.
            s3FileTransferRequestParamsDto.setMaxThreads(adjustIntegerValue(s3FileTransferRequestParamsDto.getMaxThreads(), MIN_THREADS, MAX_THREADS));

            // Download S3 files to the target local directory.
            s3FileTransferRequestParamsDto.setRecursive(true);
            cleanUpTargetLocalDirectoryOnFailure = true;
            s3Service.downloadDirectory(s3FileTransferRequestParamsDto);

            // Validate the downloaded files.
            storageFileHelper.validateDownloadedS3Files(s3FileTransferRequestParamsDto.getLocalPath(), s3KeyPrefixInformation.getS3KeyPrefix(), storageUnit);

            // Log a list of files downloaded to the target local directory.
            if (LOGGER.isInfoEnabled())
            {
                logLocalDirectoryContents(targetLocalDirectory);
            }

            // Create a downloader output manifest file.
            DownloaderOutputManifestDto downloaderOutputManifestDto =
                createDownloaderOutputManifestDto(businessObjectData, storageUnit, s3KeyPrefixInformation.getS3KeyPrefix());
            manifestWriter.writeJsonManifest(targetLocalDirectory, OUTPUT_MANIFEST_FILE_NAME, downloaderOutputManifestDto);
        }
        catch (InterruptedException | JAXBException | IOException | URISyntaxException e)
        {
            // If we got to the point of validating the target local directory being empty before this failure
            // occurred, let's rollback the data transfer by cleaning up the local target directory.
            if (cleanUpTargetLocalDirectoryOnFailure)
            {
                LOGGER.info(String.format("Rolling back the S3 data transfer by cleaning up \"%s\" target local directory.", targetLocalDirectory));
                HerdFileUtils.cleanDirectoryIgnoreException(targetLocalDirectory);
            }

            throw e;
        }
    }

    /**
     * Logs all files found in the specified local directory.
     *
     * @param directory the target local directory
     */
    private void logLocalDirectoryContents(File directory)
    {
        Collection<File> files = HerdFileUtils.listFiles(directory, TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE);
        LOGGER.info(String.format("Found %d files in \"%s\" target local directory:", files.size(), directory.getPath()));

        for (File file : files)
        {
            LOGGER.info(String.format("    %s", file.getPath()));
        }
    }

    /**
     * Creates a downloader output manifest instance from the business object data.
     *
     * @param businessObjectData the business object data that we need to create the manifest for
     * @param storageUnit the S3 storage unit for this business object data
     * @param s3KeyPrefix the S3 key prefix for this business object data
     *
     * @return the created downloader output manifest instance
     */
    protected DownloaderOutputManifestDto createDownloaderOutputManifestDto(BusinessObjectData businessObjectData, StorageUnit storageUnit, String s3KeyPrefix)
    {
        DownloaderOutputManifestDto downloaderOutputManifestDto = new DownloaderOutputManifestDto();

        // Populate basic fields.
        downloaderOutputManifestDto.setNamespace(businessObjectData.getNamespace());
        downloaderOutputManifestDto.setBusinessObjectDefinitionName(businessObjectData.getBusinessObjectDefinitionName());
        downloaderOutputManifestDto.setBusinessObjectFormatUsage(businessObjectData.getBusinessObjectFormatUsage());
        downloaderOutputManifestDto.setBusinessObjectFormatFileType(businessObjectData.getBusinessObjectFormatFileType());
        downloaderOutputManifestDto.setBusinessObjectFormatVersion(String.valueOf(businessObjectData.getBusinessObjectFormatVersion()));
        downloaderOutputManifestDto.setPartitionKey(businessObjectData.getPartitionKey());
        downloaderOutputManifestDto.setPartitionValue(businessObjectData.getPartitionValue());
        downloaderOutputManifestDto.setSubPartitionValues(businessObjectData.getSubPartitionValues());
        downloaderOutputManifestDto.setBusinessObjectDataVersion(String.valueOf(businessObjectData.getVersion()));
        downloaderOutputManifestDto.setStorageName(storageUnit.getStorage().getName());

        // Build a list of manifest files with paths relative to the S3 key prefix.
        List<ManifestFile> manifestFiles = new ArrayList<>();
        downloaderOutputManifestDto.setManifestFiles(manifestFiles);
        if (!CollectionUtils.isEmpty(storageUnit.getStorageFiles()))
        {
            for (StorageFile storageFile : storageUnit.getStorageFiles())
            {
                ManifestFile manifestFile = new ManifestFile();
                manifestFiles.add(manifestFile);
                manifestFile.setFileName(storageFile.getFilePath().replace(s3KeyPrefix, ""));
                manifestFile.setFileSizeBytes(storageFile.getFileSizeBytes());
                manifestFile.setRowCount(storageFile.getRowCount());
            }
        }

        // Populate the attributes.
        HashMap<String, String> attributes = new HashMap<>();
        if (!CollectionUtils.isEmpty(businessObjectData.getAttributes()))
        {
            for (Attribute attribute : businessObjectData.getAttributes())
            {
                attributes.put(attribute.getName(), attribute.getValue());
            }
        }
        downloaderOutputManifestDto.setAttributes(attributes);

        // Populate the business object data parents and children.
        downloaderOutputManifestDto.setBusinessObjectDataParents(businessObjectData.getBusinessObjectDataParents());
        downloaderOutputManifestDto.setBusinessObjectDataChildren(businessObjectData.getBusinessObjectDataChildren());

        return downloaderOutputManifestDto;
    }
}
