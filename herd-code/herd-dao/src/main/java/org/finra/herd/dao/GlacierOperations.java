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

import java.io.File;
import java.io.FileNotFoundException;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.glacier.transfer.ArchiveTransferManager;
import com.amazonaws.services.glacier.transfer.UploadResult;

/**
 * The AWS Glacier operations service.
 */
public interface GlacierOperations
{
    /**
     * Uploads the specified file to Amazon Glacier for archival storage in the specified vault for the user's current account. For small archives, this method
     * will upload the archive directly to Glacier. For larger archives, this method will use Glacier's multipart upload API to split the upload into multiple
     * parts for better error recovery if any errors are encountered while streaming the data to Amazon Glacier.
     *
     * @param vaultName the name of the vault to upload to
     * @param archiveDescription the description of the new archive being uploaded
     * @param file the file to upload to Amazon Glacier
     * @param archiveTransferManager the archive transfer manager
     *
     * @return the result of the upload, including the archive ID needed to access the upload later
     * @throws AmazonClientException if any problems were encountered while communicating with AWS or inside the AWS SDK for Java client code in making requests
     * or processing responses from AWS
     * @throws FileNotFoundException if the specified file to upload doesn't exist
     */
    public UploadResult upload(String vaultName, String archiveDescription, File file, ArchiveTransferManager archiveTransferManager)
        throws AmazonClientException, FileNotFoundException;
}
