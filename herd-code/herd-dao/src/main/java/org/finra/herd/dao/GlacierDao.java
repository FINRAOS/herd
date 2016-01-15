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

import java.io.FileNotFoundException;

import org.finra.herd.model.dto.GlacierArchiveTransferRequestParamsDto;
import org.finra.herd.model.dto.GlacierArchiveTransferResultsDto;

/**
 * A DAO for AWS Glacier.
 */
public interface GlacierDao
{
    /**
     * Uploads a local file into AWS Glacier.
     *
     * @param glacierArchiveTransferRequestParamsDto the Glacier archive transfer request parameters. The Glacier vault name is for the target of the copy. The
     * local file path is the local file to be copied.
     *
     * @return the Glacier archive transfer results
     * @throws InterruptedException if any problems were encountered
     * @throws FileNotFoundException if the specified file to upload doesn't exist
     */
    public GlacierArchiveTransferResultsDto uploadArchive(GlacierArchiveTransferRequestParamsDto glacierArchiveTransferRequestParamsDto)
        throws InterruptedException, FileNotFoundException;
}
