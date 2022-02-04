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

import java.util.Date;
import java.util.List;
import java.util.Properties;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.S3VersionSummary;
import com.amazonaws.services.s3.model.Tag;

import org.finra.herd.model.dto.BatchJobConfigDto;
import org.finra.herd.model.dto.S3FileCopyRequestParamsDto;
import org.finra.herd.model.dto.S3FileTransferRequestParamsDto;
import org.finra.herd.model.dto.S3FileTransferResultsDto;
import org.finra.herd.model.dto.S3ObjectTaggerRoleParamsDto;

/**
 * A DAO for Amazon AWS S3.
 */
public interface S3Dao
{
    /**
     * The AWS signer type value to use when signing with Signature Version 4. This constant needs to be the same as what is defined in
     * AmazonS3Client.S3_V4_SIGNER. Unfortunately, that one is private so we have to create our own duplicate constant here.
     */
    String SIGNER_OVERRIDE_V4 = "AWSS3V4SignerType";

    /**
     * Aborts any multipart uploads that were initiated in the specified S3 storage older than threshold date.
     *
     * @param s3FileTransferRequestParamsDto the S3 file transfer request parameters. The S3 bucket name specifies the name of the bucket containing the
     * multipart uploads to abort.
     * @param thresholdDate the date indicating which multipart uploads should be aborted
     *
     * @return the total number of aborted multipart uploads
     */
    int abortMultipartUploads(S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto, Date thresholdDate);

    /**
     * Copies an S3 object from the source S3 bucket to the same path in target bucket.  This method does not delete the source S3 object.
     *
     * @param s3FileCopyRequestParamsDto the S3 file copy request parameters.
     *
     * @return the results.
     *
     * @throws InterruptedException if any problems were encountered.
     */
    S3FileTransferResultsDto copyFile(S3FileCopyRequestParamsDto s3FileCopyRequestParamsDto) throws InterruptedException;

    /**
     * Creates an S3 object of 0 byte size that represents a directory.
     *
     * @param s3FileTransferRequestParamsDto the S3 file transfer request parameters. The S3 bucket name and S3 key prefix identify the S3 object to be
     * created.
     */
    void createDirectory(S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto);

    /**
     * Creates an S3 object of 0 byte size that represents an empty directory.
     *
     * @param s3FileTransferRequestParamsDto the S3 file transfer request parameters. The S3 bucket name and S3 key prefix identify the S3 object to be
     * created.
     */
    void createEmptyDirectory(S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto);

    /**
     * Deletes keys/key versions from specified bucket with matching prefix.
     *
     * @param s3FileTransferRequestParamsDto the S3 file transfer request parameters. The S3 bucket name and S3 key prefix identify the S3 objects to be
     * deleted.
     */
    void deleteDirectory(S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto);

    /**
     * Deletes a list of keys from specified bucket.
     *
     * @param s3FileTransferRequestParamsDto the S3 file transfer request parameters. The S3 bucket name and the file list identify the S3 objects to be
     * deleted.
     */
    void deleteFileList(S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto);

    /**
     * Downloads a directory from S3 to the local file system.
     *
     * @param s3FileTransferRequestParamsDto the S3 file transfer request parameters. The S3 bucket name and S3 key prefix are for the target of the copy. The
     * local path is the local file to be copied.
     *
     * @return the results.
     *
     * @throws InterruptedException if any problems were encountered.
     */
    S3FileTransferResultsDto downloadDirectory(S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto) throws InterruptedException;

    /**
     * Downloads a file from S3 to the local file system.
     *
     * @param s3FileTransferRequestParamsDto the S3 file transfer request parameters. The S3 bucket name and S3 key prefix are for the target of the copy. The
     * local path is the local file to be copied.
     *
     * @return the results.
     *
     * @throws InterruptedException if any problems were encountered.
     */
    S3FileTransferResultsDto downloadFile(S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto) throws InterruptedException;

    /**
     * Generates a GET pre-signed URL for the given object in S3 identified by its bucket name and key. Uses the proxy information and signer override specified
     * in the given {@link S3FileTransferRequestParamsDto}.
     *
     * @param bucketName The bucket name of the object
     * @param key The S3 key to the object
     * @param expiration The expiration date at which point the new pre-signed URL will no longer be accepted by Amazon S3.
     * @param s3FileTransferRequestParamsDto The parameters which contain information on how to use S3
     *
     * @return a pre-signed URL
     */
    String generateGetObjectPresignedUrl(String bucketName, String key, Date expiration, S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto);

    /**
     * Gets the metadata for the specified Amazon S3 object without actually fetching the object itself.
     *
     * @param s3FileTransferRequestParamsDto the S3 file transfer request parameters.  The S3 bucket name and S3 key prefix identify the S3 object's whose
     * metadata is being retrieved.
     *
     * @return null if object key is not found, otherwise all Amazon S3 object metadata for the specified object
     */
    ObjectMetadata getObjectMetadata(S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto);

    /**
     * Gets an object from S3 and parses it as a {@link Properties}.
     *
     * @param bucketName The S3 bucket name
     * @param key The S3 object key
     * @param s3FileTransferRequestParamsDto {@link S3FileTransferRequestParamsDto} with proxy information
     *
     * @return {@link Properties}
     */
    Properties getProperties(String bucketName, String key, S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto);

    /**
     * Lists all S3 objects matching the S3 key prefix in the given bucket (S3 bucket name).
     *
     * @param s3FileTransferRequestParamsDto the S3 file transfer request parameters. The S3 bucket name and S3 key prefix identify the S3 objects to get
     * listed.
     *
     * @return the list of all S3 objects represented by S3 object summaries that match the prefix in the given bucket.
     */
    List<S3ObjectSummary> listDirectory(S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto);

    /**
     * Lists all S3 objects matching the S3 key prefix in the given bucket (S3 bucket name).
     *
     * @param s3FileTransferRequestParamsDto the S3 file transfer request parameters. The S3 bucket name and S3 key prefix identify the S3 objects to get
     * listed.
     * @param ignoreZeroByteDirectoryMarkers specifies whether to ignore 0 byte objects that represent S3 directories.
     *
     * @return the list of all S3 objects represented by S3 object summaries that match the prefix in the given bucket.
     */
    List<S3ObjectSummary> listDirectory(S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto, boolean ignoreZeroByteDirectoryMarkers);

    /**
     * Lists all S3 versions matching the S3 key prefix in the given bucket (S3 bucket name). The S3 bucket name and S3 key prefix that identify the S3 versions
     * to get listed are taken from the S3 file transfer request parameters DTO.
     *
     * @param params the S3 file transfer request parameters
     *
     * @return the list of all S3 versions that match the prefix in the given bucket
     */
    List<S3VersionSummary> listVersions(final S3FileTransferRequestParamsDto params);

    /**
     * Requests to restore a list of keys in the specified bucket.
     *
     * @param s3FileTransferRequestParamsDto the S3 file transfer request parameters. The S3 bucket name and the file list identify the S3 objects to be
     * restored
     * @param expirationInDays the time, in days, between when an object is restored to the bucket and when it expires
     * @param archiveRetrievalOption the archive retrieval option when restoring an archived object
     */
    void restoreObjects(final S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto, int expirationInDays, String archiveRetrievalOption);

    /**
     * Validates that file exists in S3 using S3Client.getObject() method.
     *
     * @param s3FileTransferRequestParamsDto the S3 file transfer request parameters. The S3 bucket name and S3 key prefix identify the S3 file
     *
     * @return true if the S3 file exists, false otherwise
     *
     * @throws RuntimeException if file existence check fails
     */
    boolean s3FileExists(S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto) throws RuntimeException;

    /**
     * Tags S3 objects with the specified S3 object tag.
     *
     * @param s3FileTransferRequestParamsDto the S3 file transfer request parameters. This set of parameters contains the S3 bucket name
     * @param s3ObjectTaggerRoleParamsDto the S3 objects tagger role parameters DTO
     * @param s3ObjectSummaries the list of S3 objects to be tagged
     * @param tag the S3 object tag
     */
    void tagObjects(final S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto, final S3ObjectTaggerRoleParamsDto s3ObjectTaggerRoleParamsDto,
        final List<S3ObjectSummary> s3ObjectSummaries, final Tag tag);

    /**
     * Tags S3 versions with the specified S3 object tag.
     *
     * @param s3FileTransferRequestParamsDto the S3 file transfer request parameters. This set of parameters contains the S3 bucket name
     * @param s3ObjectTaggerRoleParamsDto the S3 objects tagger role parameters DTO
     * @param s3VersionSummaries the list of S3 versions to be tagged
     * @param tag the S3 object tag
     */
    void tagVersions(final S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto, final S3ObjectTaggerRoleParamsDto s3ObjectTaggerRoleParamsDto,
        final List<S3VersionSummary> s3VersionSummaries, final Tag tag);

    /**
     * Create the S3 batch job to replace S3 object version tags with the specified S3 object tag.
     *
     * @param s3FileTransferRequestParamsDto the S3 file transfer request parameters. The S3 bucket name and S3 key prefix.
     * @param batchJobConfig the configuration parameters used to create batch job
     * @param s3VersionSummaries the list of S3 versions to be tagged
     * @param tag the S3 object tag
     */
    void batchTagVersions(final S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto, final BatchJobConfigDto batchJobConfig,
        final List<S3VersionSummary> s3VersionSummaries, final Tag tag);

    /**
     * Uploads a local directory of files into S3.
     *
     * @param s3FileTransferRequestParamsDto the S3 file transfer request parameters. The S3 bucket name and S3 key prefix are for the target of the copy. The
     * local path is the local file to be copied.
     *
     * @return the results.
     *
     * @throws InterruptedException if any problems were encountered.
     */
    S3FileTransferResultsDto uploadDirectory(S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto) throws InterruptedException;

    /**
     * Uploads a local file into S3.
     *
     * @param s3FileTransferRequestParamsDto the S3 file transfer request parameters. The S3 bucket name and S3 key prefix are for the target of the copy. The
     * local path is the local file to be copied.
     *
     * @return the file transfer results.
     *
     * @throws InterruptedException if any problems were encountered.
     */
    S3FileTransferResultsDto uploadFile(S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto) throws InterruptedException;

    /**
     * Uploads a list of local files into S3.
     *
     * @param s3FileTransferRequestParamsDto the S3 file transfer request parameters. The S3 bucket name, S3 key prefix, and the file list (files) are for the
     * target of the copy. The local path and the file list (files) are the local files to be copied. The keys of the files are calculated relative to the
     * common parent directory (local path) and the S3 key prefix.
     *
     * @return the file transfer results.
     *
     * @throws InterruptedException if any problems were encountered.
     */
    S3FileTransferResultsDto uploadFileList(S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto) throws InterruptedException;

    /**
     * Validates that all specified Glacier storage class files are restored.
     *
     * @param s3FileTransferRequestParamsDto the S3 file transfer request parameters. The S3 bucket name and the file list identify the S3 objects to be
     * validated
     *
     * @throws RuntimeException if file validation fails
     */
    void validateGlacierS3FilesRestored(S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto) throws RuntimeException;

    /**
     * Validates that file exists in S3 and has size that equals to a specified value.
     *
     * @param s3FileTransferRequestParamsDto the S3 file transfer request parameters. The S3 bucket name and S3 key prefix identify the S3 file.
     * @param fileSizeInBytes the expected file size in bytes
     *
     * @throws RuntimeException if file validation fails
     */
    void validateS3File(S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto, Long fileSizeInBytes) throws RuntimeException;

    /**
     * Create the S3 batch job to restore a list of keys in the specified bucket.
     *
     * @param s3FileTransferRequestParamsDto the S3 file transfer request parameters. The S3 bucket name and the file list identify the S3 objects to be
     * restored
     * @param batchJobConfig the configuration parameters used to create batch job
     * @param expirationInDays the time, in days, between when an object is restored to the bucket and when it expires
     * @param archiveRetrievalOption the archive retrieval option when restoring an archived object
     */
    void batchRestoreObjects(final S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto, BatchJobConfigDto batchJobConfig, int expirationInDays,
        String archiveRetrievalOption);
}
