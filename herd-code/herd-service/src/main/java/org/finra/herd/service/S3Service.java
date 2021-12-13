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

import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.S3VersionSummary;
import com.amazonaws.services.s3.model.Tag;

import org.finra.herd.model.dto.S3FileCopyRequestParamsDto;
import org.finra.herd.model.dto.S3FileTransferRequestParamsDto;
import org.finra.herd.model.dto.S3FileTransferResultsDto;

/**
 * A service for Amazon AWS S3.
 */
public interface S3Service
{
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
     * Deletes keys/objects from specified bucket with matching prefix.  This method does not fail in case cleaning is unsuccessful, but simply logs the
     * exception information as a warning.
     *
     * @param s3FileTransferRequestParamsDto the S3 file transfer request parameters. The S3 bucket name and S3 key prefix identify the S3 objects to be
     * deleted.
     */
    void deleteDirectoryIgnoreException(S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto);

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
     * @param s3FileTransferRequestParamsDto the S3 file transfer request parameters. The S3 bucket name and S3 key prefix are for the source of the copy. The
     * local path is the local directory where the downloaded files will be copied into.
     *
     * @return the results.
     *
     * @throws InterruptedException if any problems were encountered.
     */
    S3FileTransferResultsDto downloadDirectory(S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto) throws InterruptedException;

    /**
     * Downloads a file from S3 to the local file system.
     *
     * @param s3FileTransferRequestParamsDto the S3 file transfer request parameters. The S3 bucket name and S3 key prefix are for the source of the copy. The
     * local path is the local file name where the downloaded file will be copied into.
     *
     * @return the results.
     *
     * @throws InterruptedException if any problems were encountered.
     */
    S3FileTransferResultsDto downloadFile(S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto) throws InterruptedException;

    /**
     * Lists all S3 objects matching the S3 key prefix in the given bucket (S3 bucket name).
     *
     * @param s3FileTransferRequestParamsDto the S3 file transfer request parameters. The S3 bucket name and S3 key prefix identify the S3 objects to get
     * listed.
     *
     * @return the list of all S3 objects represented by S3 object summary that match the prefix in the given bucket.
     */
    List<S3ObjectSummary> listDirectory(S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto);

    /**
     * Lists all S3 objects matching the S3 key prefix in the given bucket (S3 bucket name).
     *
     * @param s3FileTransferRequestParamsDto the S3 file transfer request parameters. The S3 bucket name and S3 key prefix identify the S3 objects to get
     * listed.
     * @param ignoreZeroByteDirectoryMarkers specifies whether to ignore 0 byte objects that represent S3 directories.
     *
     * @return the list of all keys represented by S3 object summary that match the prefix in the given bucket.
     */
    List<S3ObjectSummary> listDirectory(S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto, boolean ignoreZeroByteDirectoryMarkers);

    /**
     * Lists all S3 versions matching the S3 key prefix in the given bucket (S3 bucket name).
     *
     * @param s3FileTransferRequestParamsDto the S3 file transfer request parameters. The S3 bucket name and S3 key prefix identify the S3 versions to get
     * listed.
     *
     * @return the list of all S3 versions that match the prefix in the given bucket
     */
    List<S3VersionSummary> listVersions(S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto);

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
     * Tags S3 objects with the specified S3 object tag.
     *
     * @param s3FileTransferRequestParamsDto the S3 file transfer request parameters. This set of parameters contains the S3 bucket name
     * @param s3ObjectTaggerParamsDto the S3 file transfer request parameters to be used for tagging S3 objects
     * @param s3ObjectSummaries the list of S3 objects to be tagged
     * @param tag the S3 object tag
     */
    void tagObjects(final S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto, final S3FileTransferRequestParamsDto s3ObjectTaggerParamsDto,
        final List<S3ObjectSummary> s3ObjectSummaries, final Tag tag);

    /**
     * Tags S3 versions with the specified S3 object tag.
     *
     * @param s3FileTransferRequestParamsDto the S3 file transfer request parameters
     * @param s3ObjectTaggerParamsDto the S3 file transfer request parameters to be used for tagging S3 objects
     * @param s3VersionSummaries the list of S3 versions to be tagged
     * @param tag the S3 object tag
     */
    void tagVersions(final S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto, final S3FileTransferRequestParamsDto s3ObjectTaggerParamsDto,
        final List<S3VersionSummary> s3VersionSummaries, final Tag tag);

    /**
     * Uploads a local directory of files into S3.
     *
     * @param s3FileTransferRequestParamsDto the S3 file transfer request parameters. The S3 bucket name and S3 key prefix are for the target of the copy. The
     * local path is the local directory to be copied.
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
     * @return the results.
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
}
