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

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.model.dto.S3FileTransferRequestParamsDto;

@Component
public class S3DaoTestHelper
{
    @Autowired
    private S3Dao s3Dao;

    @Autowired
    private StorageDaoTestHelper storageDaoTestHelper;

    /**
     * Returns an S3 file transfer request parameters DTO instance initialized using hard coded test values. This DTO is required for testing and clean up
     * activities.
     *
     * @return the newly created S3 file transfer request parameters DTO
     */
    public S3FileTransferRequestParamsDto getTestS3FileTransferRequestParamsDto()
    {
        String s3BucketName = storageDaoTestHelper.getS3ManagedBucketName();

        return S3FileTransferRequestParamsDto.builder().s3BucketName(s3BucketName).s3KeyPrefix(AbstractDaoTest.TEST_S3_KEY_PREFIX).build();
    }

    /**
     * Validates uploaded S3 files.
     *
     * @param s3FileTransferRequestParamsDto the S3 file transfer request parameters DTO
     * @param expectedS3Keys the list of expected S3 keys
     */
    public void validateS3FileUpload(S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto, List<String> expectedS3Keys)
    {
        // Validate the upload.
        List<S3ObjectSummary> s3ObjectSummaries = s3Dao.listDirectory(s3FileTransferRequestParamsDto);
        assertTrue(s3ObjectSummaries.size() == expectedS3Keys.size());

        // Build a list of the actual S3 keys.
        List<String> actualS3Keys = new ArrayList<>();
        for (S3ObjectSummary s3ObjectSummary : s3ObjectSummaries)
        {
            actualS3Keys.add(s3ObjectSummary.getKey());
        }

        // Check that all local test files got uploaded.
        assertTrue(expectedS3Keys.containsAll(actualS3Keys));
        assertTrue(actualS3Keys.containsAll(expectedS3Keys));
    }
}
