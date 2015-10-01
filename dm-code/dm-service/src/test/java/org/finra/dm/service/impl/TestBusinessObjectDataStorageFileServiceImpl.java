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

import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import org.finra.dm.dao.config.DaoSpringModuleConfig;
import org.finra.dm.model.api.xml.BusinessObjectDataStorageFilesCreateRequest;
import org.finra.dm.model.api.xml.BusinessObjectDataStorageFilesCreateResponse;
import org.finra.dm.service.BusinessObjectDataStorageFileService;
import org.finra.dm.service.impl.BusinessObjectDataStorageFileServiceImpl;

/**
 * This is a Business Object Data Storage File service implementation for testing.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.DM_TRANSACTION_MANAGER_BEAN_NAME)
@Primary
public class TestBusinessObjectDataStorageFileServiceImpl extends BusinessObjectDataStorageFileServiceImpl implements BusinessObjectDataStorageFileService
{
    // Overwrite the base class method to change transactional attributes.
    @Override
    public BusinessObjectDataStorageFilesCreateResponse createBusinessObjectDataStorageFiles(BusinessObjectDataStorageFilesCreateRequest request)
    {
        return createBusinessObjectDataStorageFilesImpl(request);
    }
}
