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
package org.finra.herd.tools.uploader;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import javax.xml.bind.JAXBException;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.IterableUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.core.HerdFileUtils;
import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.core.helper.HerdThreadHelper;
import org.finra.herd.dao.helper.JsonHelper;
import org.finra.herd.model.api.xml.AwsCredential;
import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectDataVersion;
import org.finra.herd.model.api.xml.BusinessObjectDataVersions;
import org.finra.herd.model.api.xml.Storage;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.ManifestFile;
import org.finra.herd.model.dto.RegServerAccessParamsDto;
import org.finra.herd.model.dto.S3FileTransferRequestParamsDto;
import org.finra.herd.model.dto.UploaderInputManifestDto;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.herd.service.helper.BusinessObjectDataHelper;
import org.finra.herd.service.helper.StorageHelper;
import org.finra.herd.tools.common.databridge.AutoRefreshCredentialProvider;
import org.finra.herd.tools.common.databridge.DataBridgeController;

/**
 * Executes the UploaderApp workflow.
 */
@Component
public class UploaderController extends DataBridgeController
{
    private static final Logger LOGGER = LoggerFactory.getLogger(UploaderController.class);

    @Autowired
    private BusinessObjectDataHelper businessObjectDataHelper;

    @Autowired
    private ConfigurationHelper configurationHelper;

    @Autowired
    private HerdThreadHelper herdThreadHelper;

    @Autowired
    private JsonHelper jsonHelper;

    @Autowired
    private UploaderManifestReader manifestReader;

    @Autowired
    private StorageHelper storageHelper;

    @Autowired
    private UploaderWebClient uploaderWebClient;

    /**
     * Executes the uploader workflow.
     *
     * @param regServerAccessParamsDto the DTO for the parameters required to communicate with the registration server
     * @param manifestPath the local path to the manifest file
     * @param params the S3 file transfer request parameters being used to pass the following arguments: <ul> <li><code>s3AccessKey</code> the S3 access key
     * <li><code>s3SecretKey</code> the S3 secret key <li><code>localPath</code> the local path to directory containing data files
     * <li><code>httpProxyHost</code> the HTTP proxy host <li><code>httpProxyPort</code> the HTTP proxy port <li><code>maxThreads</code> the maximum number of
     * threads to use for file transfer to S3< <li><code>useRrs</code> specifies whether S3 reduced redundancy storage option will be used when copying to S3
     * </ul>
     * @param createNewVersion if not set, only initial version of the business object data is allowed to be created
     * @param force if set, allows upload to proceed when the latest version of the business object data has UPLOADING status by invalidating that version
     * @param maxRetryAttempts the maximum number of the business object data registration retry attempts
     * @param retryDelaySecs the delay in seconds between the business object data registration retry attempts
     *
     * @throws InterruptedException if the upload thread was interrupted
     * @throws JAXBException if a JAXB error was encountered
     * @throws IOException if an I/O error was encountered
     * @throws URISyntaxException if a URI syntax error was encountered
     * @throws KeyStoreException if a key store exception occurs
     * @throws NoSuchAlgorithmException if a no such algorithm exception occurs
     * @throws KeyManagementException if key management exception
     */
    @SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST_OF_RETURN_VALUE",
        justification = "manifestReader.readJsonManifest will always return an UploaderInputManifestDto object.")
    public void performUpload(RegServerAccessParamsDto regServerAccessParamsDto, File manifestPath, S3FileTransferRequestParamsDto params,
        Boolean createNewVersion, Boolean force, Integer maxRetryAttempts, Integer retryDelaySecs)
        throws InterruptedException, JAXBException, IOException, URISyntaxException, NoSuchAlgorithmException, KeyStoreException, KeyManagementException
    {
        boolean cleanUpS3KeyPrefixOnFailure = false;
        BusinessObjectDataKey businessObjectDataKey = null;

        try
        {
            // Process manifest file
            UploaderInputManifestDto manifest = manifestReader.readJsonManifest(manifestPath);

            String storageName = getStorageNameFromManifest(manifest);
            manifest.setStorageName(storageName);

            // Validate local files and prepare a list of source files to copy to S3.
            List<File> sourceFiles = getValidatedLocalFiles(params.getLocalPath(), manifest.getManifestFiles());

            // Validate that we do not have duplicate files listed in the manifest file.
            List<File> duplicateFiles = findDuplicateFiles(sourceFiles);

            if (!duplicateFiles.isEmpty())
            {
                throw new IllegalArgumentException(
                    String.format("Manifest contains duplicate file names. Duplicates: [\"%s\"]", StringUtils.join(duplicateFiles, "\", \"")));
            }

            // Initialize uploader web client.
            uploaderWebClient.setRegServerAccessParamsDto(regServerAccessParamsDto);

            // Handle the latest business object data version if one exists.
            checkLatestBusinessObjectDataVersion(manifest, force);

            // Pre-register a new version of business object data in UPLOADING state with the registration server.
            BusinessObjectData businessObjectData = uploaderWebClient.preRegisterBusinessObjectData(manifest, storageName, createNewVersion);

            // Get business object data key.
            businessObjectDataKey = businessObjectDataHelper.getBusinessObjectDataKey(businessObjectData);

            // Get the business object data version.
            Integer businessObjectDataVersion = businessObjectDataKey.getBusinessObjectDataVersion();

            // Add credential provider.
            params.getAdditionalAwsCredentialsProviders().add(new AutoRefreshCredentialProvider()
            {
                @Override
                public AwsCredential getNewAwsCredential() throws Exception
                {
                    return uploaderWebClient.getBusinessObjectDataUploadCredential(manifest, storageName, businessObjectDataVersion, null).getAwsCredential();
                }
            });

            // Get S3 key prefix from the business object data pre-registration response.
            String s3KeyPrefix = IterableUtils.get(businessObjectData.getStorageUnits(), 0).getStorageDirectory().getDirectoryPath();

            // Get S3 bucket information.
            Storage storage = uploaderWebClient.getStorage(storageName);

            // Get S3 bucket name.  Please note that since this value is required we pass a "true" flag.
            String s3BucketName =
                storageHelper.getStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), storage, true);

            // Set the KMS ID, if available
            String kmsKeyId =
                storageHelper.getStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_KMS_KEY_ID), storage, false);
            params.setKmsKeyId(kmsKeyId);

            // Special handling for the maxThreads command line option.
            params.setMaxThreads(adjustIntegerValue(params.getMaxThreads(), MIN_THREADS, MAX_THREADS));

            // Populate several missing fields in the S3 file transfer request parameters DTO.
            params.setS3BucketName(s3BucketName);
            // Since the S3 key prefix represents a directory, we add a trailing '/' character to it.
            params.setS3KeyPrefix(s3KeyPrefix + "/");
            params.setFiles(sourceFiles);

            // Check if the destination S3 key prefix is empty.
            // When listing S3 files, by default, we do not ignore 0 byte objects that represent S3 directories.
            if (s3Service.listDirectory(params).isEmpty())
            {
                cleanUpS3KeyPrefixOnFailure = true;
            }
            else
            {
                throw new IllegalStateException(String
                    .format("The destination S3 folder is not empty. S3 Bucket Name: \"%s\". S3 key prefix: \"%s\".", params.getS3BucketName(),
                        params.getS3KeyPrefix()));
            }

            // Upload files.
            s3Service.uploadFileList(params);

            // Get the list of files uploaded to S3 key prefix.
            if (LOGGER.isInfoEnabled())
            {
                logS3KeyPrefixContents(params);
            }

            // Add storage files to the business object data.
            addStorageFilesWithRetry(businessObjectDataKey, manifest, params, storage.getName(), maxRetryAttempts, retryDelaySecs);

            // Change status of the business object data to VALID.
            uploaderWebClient.updateBusinessObjectDataStatus(businessObjectDataKey, BusinessObjectDataStatusEntity.VALID);
        }
        catch (InterruptedException | JAXBException | IOException | URISyntaxException e)
        {
            // If we got to the point of checking the target S3 key prefix before this failure
            // occurred, let's rollback the data transfer (clean up the S3 key prefix).
            if (cleanUpS3KeyPrefixOnFailure)
            {
                LOGGER.info(String
                    .format("Rolling back the S3 data transfer by deleting keys/objects with prefix \"%s\" from bucket \"%s\".", params.getS3KeyPrefix(),
                        params.getS3BucketName()));
                s3Service.deleteDirectoryIgnoreException(params);
            }

            // If a new business object data version got pre-registered, update it's status to INVALID.
            if (businessObjectDataKey != null)
            {
                uploaderWebClient.updateBusinessObjectDataStatusIgnoreException(businessObjectDataKey, BusinessObjectDataStatusEntity.INVALID);
            }

            throw e;
        }
    }

    /**
     * Add storage files to a business object data with a retry on error.
     *
     * @param businessObjectDataKey the business object data key
     * @param manifest the uploader input manifest
     * @param params the S3 file transfer parameters
     * @param storageName the name of the storage
     * @param maxRetryAttempts Maximum number of retry attempts on error
     * @param retryDelaySecs Delay in seconds between retries
     *
     * @throws JAXBException if a JAXB error was encountered
     * @throws IOException if an I/O error was encountered
     * @throws URISyntaxException if a URI syntax error was encountered
     * @throws KeyStoreException if a key store exception occurs
     * @throws NoSuchAlgorithmException if a no such algorithm exception occurs
     * @throws KeyManagementException if key management exception
     */
    private void addStorageFilesWithRetry(BusinessObjectDataKey businessObjectDataKey, UploaderInputManifestDto manifest, S3FileTransferRequestParamsDto params,
        String storageName, Integer maxRetryAttempts, Integer retryDelaySecs)
        throws IOException, JAXBException, URISyntaxException, NoSuchAlgorithmException, KeyStoreException, KeyManagementException
    {
        // Initialize a retry count to know the number of times we re-try calling the method.
        int retryCount = 0;

        // Loop indefinitely. We will exit the loop by returning within the loop or throwing an exception at some point.
        while (true)
        {
            try
            {
                // Attempt to register data with the registration server.
                uploaderWebClient.addStorageFiles(businessObjectDataKey, manifest, params, storageName);
                break;
            }
            catch (IOException | JAXBException | URISyntaxException e)
            {
                // Check if we've retried enough times.
                if (retryCount >= maxRetryAttempts)
                {
                    // We've retried enough times so rethrow the original exception.
                    LOGGER.warn(
                        "An exception occurred when adding storage files to the business object data. The maximum number of retries of " + maxRetryAttempts +
                            " has been exceeded so the exception will now be thrown.");
                    throw e;
                }
                else
                {
                    // Log a warning.
                    LOGGER.warn("An exception occurred when adding storage files to the business object data. {}", e.toString(), e);
                    LOGGER.warn("Will retry in " + retryDelaySecs + " second(s) and no more than " + (maxRetryAttempts - retryCount) + " more time(s).");

                    // We can retry again so increment a counter to keep track of the number of times we retried.
                    retryCount++;

                    // Sleep for the specified delay interval.
                    herdThreadHelper.sleep(retryDelaySecs * 1000L);
                }
            }
        }
    }

    /**
     * Handles the uploader logic regarding the latest business object data version if one exists.
     *
     * @param manifest the uploader input manifest
     * @param force if set, allows upload to proceed when the latest version of the business object data has UPLOADING status by invalidating that version
     *
     * @throws JAXBException if a JAXB error was encountered
     * @throws IOException if an I/O error was encountered
     * @throws URISyntaxException if a URI syntax error was encountered
     * @throws KeyStoreException if a key store exception occurs
     * @throws NoSuchAlgorithmException if a no such algorithm exception occurs
     * @throws KeyManagementException if key management exception
     */
    private void checkLatestBusinessObjectDataVersion(UploaderInputManifestDto manifest, Boolean force)
        throws JAXBException, IOException, URISyntaxException, NoSuchAlgorithmException, KeyStoreException, KeyManagementException
    {
        // Retrieve all already registered versions for this business object data.
        BusinessObjectDataVersions businessObjectDataVersions = uploaderWebClient.getBusinessObjectDataVersions(
            new BusinessObjectDataKey(manifest.getNamespace(), manifest.getBusinessObjectDefinitionName(), manifest.getBusinessObjectFormatUsage(),
                manifest.getBusinessObjectFormatFileType(), Integer.valueOf(manifest.getBusinessObjectFormatVersion()), manifest.getPartitionValue(),
                manifest.getSubPartitionValues(), null));

        // Check if the latest version of the business object data.
        if (CollectionUtils.isNotEmpty(businessObjectDataVersions.getBusinessObjectDataVersions()))
        {
            BusinessObjectDataVersion latestBusinessObjectDataVersion =
                businessObjectDataVersions.getBusinessObjectDataVersions().get(businessObjectDataVersions.getBusinessObjectDataVersions().size() - 1);

            // Check if the latest version of the business object data is in UPLOADING state.
            if (BusinessObjectDataStatusEntity.UPLOADING.equals(latestBusinessObjectDataVersion.getStatus()))
            {
                LOGGER.info(String.format("Found the latest version of the business object data in UPLOADING state. businessObjectDataKey=%s",
                    jsonHelper.objectToJson(latestBusinessObjectDataVersion.getBusinessObjectDataKey())));

                if (force)
                {
                    // If the "force" flag is set, change the status of the latest business object data version to INVALID.
                    uploaderWebClient
                        .updateBusinessObjectDataStatus(latestBusinessObjectDataVersion.getBusinessObjectDataKey(), BusinessObjectDataStatusEntity.INVALID);
                }
                else
                {
                    // Fail the upload due to the status of the latest business object data version being UPLOADING.
                    throw new IllegalArgumentException(String.format(
                        "Unable to register business object data because the latest business object data version is detected in UPLOADING state. " +
                            "Please use -force option to invalidate the latest business object version and allow upload to proceed. " +
                            "Business object data {%s}",
                        businessObjectDataHelper.businessObjectDataKeyToString(latestBusinessObjectDataVersion.getBusinessObjectDataKey())));
                }
            }
        }
    }

    /**
     * Returns a list of all duplicate files found in the specified list of files.
     *
     * @param files the list of files to be checked for duplicates
     *
     * @return the list of duplicate files found, otherwise an empty list of files
     */
    private List<File> findDuplicateFiles(List<File> files)
    {
        HashSet<File> sourceFileSet = new HashSet<>();
        List<File> duplicateFiles = new ArrayList<>();

        for (File file : files)
        {
            if (!sourceFileSet.contains(file))
            {
                sourceFileSet.add(file);
            }
            else
            {
                duplicateFiles.add(file);
            }
        }

        return duplicateFiles;
    }

    /**
     * Returns the list of File objects created from the specified list of local files after they are validated for existence and read access.
     *
     * @param localDir the local path to directory to be used to construct the relative absolute paths for the files to be validated
     * @param manifestFiles the list of manifest files that contain file paths relative to <code>localDir</code> to be validated.
     *
     * @return the list of validated File objects
     * @throws IllegalArgumentException if <code>localDir</code> or local files are not valid
     * @throws IOException if there is a filesystem query issue to construct a canonical form of an abstract file path
     */
    private List<File> getValidatedLocalFiles(String localDir, List<ManifestFile> manifestFiles) throws IllegalArgumentException, IOException
    {
        // Create a "directory" file and ensure it is valid.
        File directory = new File(localDir);

        if (!directory.isDirectory() || !directory.canRead())
        {
            throw new IllegalArgumentException(String.format("Invalid local base directory: %s", directory.getAbsolutePath()));
        }

        // For each file path from the list, create Java "File" objects to the real file location (i.e. with the directory
        // prepended to it), and verify that the file exists. If not, an IllegalArgumentException will be thrown.
        String basedir = directory.getAbsolutePath();
        List<File> resultFiles = new ArrayList<>();

        for (ManifestFile manifestFile : manifestFiles)
        {
            // Create a "real file" that points to the actual file on the file system (i.e. directory + manifest file path).
            String realFullPathFileName = Paths.get(basedir, manifestFile.getFileName()).toFile().getPath();
            realFullPathFileName = realFullPathFileName.replaceAll("\\\\", "/");
            File realFile = new File(realFullPathFileName);

            // Verify that the file exists and is readable.
            HerdFileUtils.verifyFileExistsAndReadable(realFile);

            // Verify that the name of the actual file on the file system exactly matches the name of the real file on the file system.
            // This will handle potential case issues on Windows systems. Note that the canonical file gives the actual file name on the system.
            // The non-canonical file gives the name as it was specified in the manifest.
            String realFileName = realFile.getName();
            String manifestFileName = realFile.getCanonicalFile().getName();

            if (!realFileName.equals(manifestFileName))
            {
                throw new IllegalArgumentException("Manifest filename \"" + manifestFileName + "\" does not match actual filename \"" + realFileName + "\".");
            }

            resultFiles.add(realFile);
        }

        return resultFiles;
    }

    /**
     * Logs all files found in the specified S3 location.
     *
     * @param params the S3 file transfer request parameters
     */
    private void logS3KeyPrefixContents(S3FileTransferRequestParamsDto params)
    {
        List<S3ObjectSummary> s3ObjectSummaries = s3Service.listDirectory(params);
        LOGGER.info(
            String.format("Found %d keys with prefix \"%s\" in bucket \"%s\":", s3ObjectSummaries.size(), params.getS3KeyPrefix(), params.getS3BucketName()));

        for (S3ObjectSummary s3ObjectSummary : s3ObjectSummaries)
        {
            LOGGER.info(String.format("    s3://%s/%s", params.getS3BucketName(), s3ObjectSummary.getKey()));
        }
    }
}
