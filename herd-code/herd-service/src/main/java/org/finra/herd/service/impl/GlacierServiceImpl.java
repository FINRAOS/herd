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
package org.finra.herd.service.impl;

import java.io.FileNotFoundException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import org.finra.herd.dao.GlacierDao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.dto.GlacierArchiveTransferRequestParamsDto;
import org.finra.herd.model.dto.GlacierArchiveTransferResultsDto;
import org.finra.herd.service.GlacierService;

/**
 * The AWS Glacier service implementation.
 */
@Service
// This probably won't do anything since Glacier doesn't use our transaction manager. Nonetheless, it's good to have since this is a service
// class and we might add other methods in the future which this could get used.
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class GlacierServiceImpl implements GlacierService
{
    @Autowired
    private GlacierDao glacierDao;

    /**
     * {@inheritDoc}
     */
    @Override
    public GlacierArchiveTransferResultsDto uploadArchive(GlacierArchiveTransferRequestParamsDto glacierArchiveTransferRequestParamsDto)
        throws InterruptedException, FileNotFoundException
    {
        return glacierDao.uploadArchive(glacierArchiveTransferRequestParamsDto);
    }
}
