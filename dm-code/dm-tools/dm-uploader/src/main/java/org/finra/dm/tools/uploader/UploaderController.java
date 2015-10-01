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
package org.finra.dm.tools.uploader;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import javax.xml.bind.JAXBException;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.dm.core.DmFileUtils;
import org.finra.dm.core.helper.DmThreadHelper;
import org.finra.dm.model.dto.DmRegServerAccessParamsDto;
import org.finra.dm.model.dto.ManifestFile;
import org.finra.dm.model.dto.S3FileTransferRequestParamsDto;
import org.finra.dm.model.dto.UploaderInputManifestDto;
import org.finra.dm.model.jpa.StorageAttributeEntity;
import org.finra.dm.model.jpa.StorageEntity;
import org.finra.dm.model.api.xml.S3KeyPrefixInformation;
import org.finra.dm.model.api.xml.Storage;
import org.finra.dm.model.api.xml.StorageFile;
import org.finra.dm.tools.common.databridge.DataBridgeController;

/**
 * Executes the UploaderApp workflow.
 */
@Component
public class UploaderController extends DataBridgeController
{
    private static final Logger LOGGER = Logger.getLogger(UploaderController.class);

    @Autowired
    private UploaderManifestReader manifestReader;

    @Autowired
    private UploaderWebClient uploaderWebClient;

    @Autowired
    private DmThreadHelper dmThreadHelper;

    /**
     * Executes the uploader workflow.
     *
     * @param dmRegServerAccessParamsDto the DTO for the parameters required to communicate with the Data Management Registration Server
     * @param manifestPath the local path to the manifest file
     * @param params the S3 file transfer request parameters being used to pass the following arguments: <ul> <li><code>s3AccessKey</code> the S3 access key
     * <li><code>s3SecretKey</code> the S3 secret key <li><code>localPath</code> the local path to directory containing data files
     * <li><code>httpProxyHost</code> the HTTP proxy host <li><code>httpProxyPort</code> the HTTP proxy port <li><code>maxThreads</code> the maximum number of
     * threads to use for file transfer to S3< <li><code>useRrs</code> specifies whether S3 reduced redundancy storage option will be used when copying to S3
     * </ul>
     * @param createNewVersion if not set, only initial version of the business object data is allowed to be created
     * @param maxRetryAttempts the maximum number of the business object data registration retry attempts
     * @param retryDelaySecs the delay in seconds between the business object data registration retry attempts
     *
     * @throws InterruptedException if the upload thread was interrupted.
     * @throws JAXBException if a JAXB error was encountered.
     * @throws IOException if an I/O error was encountered.
     * @throws URISyntaxException if a URI syntax error was encountered.
     */
    @SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST_OF_RETURN_VALUE",
        justification = "manifestReader.readJsonManifest will always return an UploaderInputManifestDto object.")
    public void performUpload(DmRegServerAccessParamsDto dmRegServerAccessParamsDto, File manifestPath, S3FileTransferRequestParamsDto params,
        Boolean createNewVersion, Integer maxRetryAttempts, Integer retryDelaySecs) throws InterruptedException, JAXBException, IOException, URISyntaxException
    {
        boolean cleanUpS3KeyPrefixOnFailure = false;

        try
        {
            // Process manifest file
            UploaderInputManifestDto manifest = manifestReader.readJsonManifest(manifestPath);

            // Validate local files and prepare a list of source files to copy to S3.
            List<File> sourceFiles = getValidatedLocalFiles(params.getLocalPath(), manifest.getManifestFiles());

            // Validate that we do not have duplicate files listed in the manifest file.
            List<File> duplicateFiles = findDuplicateFiles(sourceFiles);

            if (!duplicateFiles.isEmpty())
            {
                throw new IllegalArgumentException(
                    String.format("Manifest contains duplicate file names. Duplicates: [\"%s\"]", StringUtils.join(duplicateFiles, "\", \"")));
            }

            // Get S3 key prefix from the Data Management service.
            uploaderWebClient.setDmRegServerAccessParamsDto(dmRegServerAccessParamsDto);
            S3KeyPrefixInformation s3KeyPrefixInformation = uploaderWebClient.getS3KeyPrefix(manifest, createNewVersion);

            // Get S3 managed bucket information.
            Storage s3ManagedStorage = uploaderWebClient.getStorage(StorageEntity.MANAGED_STORAGE);

            // Get S3 managed bucket name.  Please note that since this value is required we pass a "true" flag.
            String s3BucketName = dmHelper.getStorageAttributeValueByName(StorageAttributeEntity.ATTRIBUTE_BUCKET_NAME, s3ManagedStorage, true);

            // Special handling for the maxThreads command line option.
            params.setMaxThreads(adjustIntegerValue(params.getMaxThreads(), MIN_THREADS, MAX_THREADS));

            // Populate several missing fields in the S3 file transfer request parameters DTO.
            params.setS3BucketName(s3BucketName);
            // Since the S3 key prefix represents a directory, we add a trailing '/' character to it.
            params.setS3KeyPrefix(s3KeyPrefixInformation.getS3KeyPrefix() + "/");
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

            // Initialize a retry count to know the number of times we re-try calling the method.
            int retryCount = 0;

            // Loop indefinitely. We will exit the loop by returning within the loop or throwing an exception at some point.
            while (true)
            {
                try
                {
                    // Attempt to register data with the Data Management service.
                    uploaderWebClient.registerBusinessObjectData(manifest, params, s3ManagedStorage.getName(), createNewVersion);
                    return;
                }
                catch (Exception e)
                {
                    // Check if we've retried enough times.
                    if (retryCount >= maxRetryAttempts)
                    {
                        // We've retried enough times so rethrow the original exception.
                        LOGGER.warn("An exception occurred when registering business object data. The maximum number of retries of " + maxRetryAttempts +
                            " has been exceeded so the exception will now be thrown.");
                        throw e;
                    }
                    else
                    {
                        // Log a warning.
                        LOGGER.warn("An exception occurred when registering business object data.", e);
                        LOGGER.warn("Will retry in " + retryDelaySecs + " second(s) and no more than " +
                            (maxRetryAttempts - retryCount) + " more time(s).");

                        // We can retry again so increment a counter to keep track of the number of times we retried.
                        retryCount++;

                        // Sleep for the specified delay interval.
                        dmThreadHelper.sleep(retryDelaySecs * 1000L);
                    }
                }
            }
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

            throw e;
        }
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
    protected List<File> getValidatedLocalFiles(String localDir, List<ManifestFile> manifestFiles) throws IllegalArgumentException, IOException
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
            DmFileUtils.verifyFileExistsAndReadable(realFile);

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
     * Returns a list of all duplicate files found in the specified list of files.
     *
     * @param files the list of files to be checked for duplicates
     *
     * @return the list of duplicate files found, otherwise an empty list of files
     */
    protected List<File> findDuplicateFiles(List<File> files)
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
     * Logs all files found in the specified S3 location.
     *
     * @param params the S3 file transfer request parameters
     */
    protected void logS3KeyPrefixContents(S3FileTransferRequestParamsDto params)
    {
        List<StorageFile> storageFiles = s3Service.listDirectory(params);
        LOGGER
            .info(String.format("Found %d keys with prefix \"%s\" in bucket \"%s\":", storageFiles.size(), params.getS3KeyPrefix(), params.getS3BucketName()));

        for (StorageFile storageFile : storageFiles)
        {
            LOGGER.info(String.format("    s3://%s/%s", params.getS3BucketName(), storageFile.getFilePath()));
        }
    }
}
