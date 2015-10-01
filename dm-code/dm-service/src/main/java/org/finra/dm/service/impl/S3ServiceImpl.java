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
package org.finra.dm.service.impl;

import java.util.List;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import org.finra.dm.dao.S3Dao;
import org.finra.dm.dao.config.DaoSpringModuleConfig;
import org.finra.dm.model.dto.S3FileCopyRequestParamsDto;
import org.finra.dm.model.dto.S3FileTransferRequestParamsDto;
import org.finra.dm.model.dto.S3FileTransferResultsDto;
import org.finra.dm.model.api.xml.StorageFile;
import org.finra.dm.service.S3Service;

/**
 * The S3 service implementation.
 */
@Service
// This probably won't do anything since S3 doesn't use our transaction manager. Nonetheless, it's good to have since this is a service
// class and we might add other methods in the future which this could get used.
@Transactional(value = DaoSpringModuleConfig.DM_TRANSACTION_MANAGER_BEAN_NAME)
public class S3ServiceImpl implements S3Service
{
    private static final Logger LOGGER = Logger.getLogger(S3ServiceImpl.class);

    @Autowired
    private S3Dao s3Dao;

    @Override
    public void createDirectory(S3FileTransferRequestParamsDto params)
    {
        s3Dao.createDirectory(params);
    }

    @Override
    public List<StorageFile> listDirectory(S3FileTransferRequestParamsDto params)
    {
        // By default, we do not ignore 0 byte objects that represent S3 directories.
        return s3Dao.listDirectory(params, false);
    }

    @Override
    public List<StorageFile> listDirectory(S3FileTransferRequestParamsDto params, boolean ignoreZeroByteDirectoryMarkers)
    {
        return s3Dao.listDirectory(params, ignoreZeroByteDirectoryMarkers);
    }

    @Override
    public S3FileTransferResultsDto uploadFile(S3FileTransferRequestParamsDto params) throws InterruptedException
    {
        return s3Dao.uploadFile(params);
    }

    @Override
    public S3FileTransferResultsDto uploadFileList(S3FileTransferRequestParamsDto params) throws InterruptedException
    {
        return s3Dao.uploadFileList(params);
    }

    @Override
    public S3FileTransferResultsDto uploadDirectory(S3FileTransferRequestParamsDto params) throws InterruptedException
    {
        return s3Dao.uploadDirectory(params);
    }

    @Override
    public S3FileTransferResultsDto copyFile(S3FileCopyRequestParamsDto params) throws InterruptedException
    {
        return s3Dao.copyFile(params);
    }

    @Override
    public void deleteFile(S3FileTransferRequestParamsDto params)
    {
        s3Dao.deleteFile(params);
    }

    @Override
    public void deleteFileList(S3FileTransferRequestParamsDto params)
    {
        s3Dao.deleteFileList(params);
    }

    @Override
    public void deleteDirectory(S3FileTransferRequestParamsDto params)
    {
        s3Dao.deleteDirectory(params);
    }

    @Override
    public void deleteDirectoryIgnoreException(S3FileTransferRequestParamsDto params)
    {
        try
        {
            s3Dao.deleteDirectory(params);
        }
        catch (Exception e)
        {
            LOGGER.warn(e.getMessage(), e);
        }
    }

    @Override
    public S3FileTransferResultsDto downloadFile(S3FileTransferRequestParamsDto params) throws InterruptedException
    {
        return s3Dao.downloadFile(params);
    }

    @Override
    public S3FileTransferResultsDto downloadDirectory(S3FileTransferRequestParamsDto params) throws InterruptedException
    {
        return s3Dao.downloadDirectory(params);
    }
}
