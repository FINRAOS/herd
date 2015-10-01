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
package org.finra.dm.model.dto;

/**
 * A DTO that contains the results of an S3 file/directory transfer.
 */
public class S3FileTransferResultsDto
{
    /**
     * The total number of files that were transferred.
     */
    private Long totalFilesTransferred;

    /**
     * The total number of bytes that were transferred.
     */
    private Long totalBytesTransferred;

    /**
     * The duration in milliseconds that it took to perform the transfer.
     */
    private Long durationMillis;

    public Long getTotalFilesTransferred()
    {
        return totalFilesTransferred;
    }

    public void setTotalFilesTransferred(Long totalFilesTransferred)
    {
        this.totalFilesTransferred = totalFilesTransferred;
    }

    public Long getTotalBytesTransferred()
    {
        return totalBytesTransferred;
    }

    public void setTotalBytesTransferred(Long totalBytesTransferred)
    {
        this.totalBytesTransferred = totalBytesTransferred;
    }

    public Long getDurationMillis()
    {
        return durationMillis;
    }

    public void setDurationMillis(Long durationMillis)
    {
        this.durationMillis = durationMillis;
    }
}
