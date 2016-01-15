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
package org.finra.herd.dao.impl;

import java.io.File;
import java.io.FileNotFoundException;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.glacier.transfer.ArchiveTransferManager;
import com.amazonaws.services.glacier.transfer.UploadResult;

import org.finra.herd.dao.GlacierOperations;

/**
 * Mock implementation of AWS Glacier operations.
 * <p/>
 * Some operations use a series of predefined prefixes to hint the operation to behave in a certain manner (throwing exceptions, for example).
 * <p/>
 * Some operations which either put or list objects, will NOT throw an exception even when a specified vault does not exist. This is because some tests are
 * assuming that the vault already exists and test may not have permissions to create test buckets during unit tests when testing against real AWS Glacier.
 */
public class MockGlacierOperationsImpl implements GlacierOperations
{
    public static final String MOCK_GLACIER_ARCHIVE_ID = "mock_glacier_archive_id";

    /**
     * Suffix to hint operation to throw an AmazonServiceException
     */
    public static final String MOCK_GLACIER_VAULT_NAME_SERVICE_EXCEPTION = "mock_glacier_vault_name_service_exception";

    @Override
    public UploadResult upload(String vaultName, String archiveDescription, File file, ArchiveTransferManager archiveTransferManager)
        throws AmazonClientException, FileNotFoundException
    {
        if (vaultName.equals(MOCK_GLACIER_VAULT_NAME_SERVICE_EXCEPTION))
        {
            throw new AmazonServiceException(null);
        }

        return new UploadResult(MOCK_GLACIER_ARCHIVE_ID);
    }
}
