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

import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.dto.StoragePolicySelection;
import org.finra.herd.model.dto.StoragePolicyTransitionParamsDto;

@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
@Primary
public class TestStoragePolicyProcessorHelperServiceImpl extends StoragePolicyProcessorHelperServiceImpl
{
    /**
     * {@inheritDoc}
     * <p/>
     * This implementation keeps the current transaction context.
     */
    @Override
    public void initiateStoragePolicyTransition(StoragePolicyTransitionParamsDto storagePolicyTransitionParamsDto,
        StoragePolicySelection storagePolicySelection)
    {
        initiateStoragePolicyTransitionImpl(storagePolicyTransitionParamsDto, storagePolicySelection);
    }

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation keeps the current transaction context.
     */
    @Override
    public void executeStoragePolicyTransition(StoragePolicyTransitionParamsDto storagePolicyTransitionParamsDto)
    {
        executeStoragePolicyTransitionImpl(storagePolicyTransitionParamsDto);
    }

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation keeps the current transaction context.
     */
    @Override
    public void completeStoragePolicyTransition(StoragePolicyTransitionParamsDto storagePolicyTransitionParamsDto)
    {
        completeStoragePolicyTransitionImpl(storagePolicyTransitionParamsDto);
    }

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation keeps the current transaction context.
     */
    @Override
    public void updateStoragePolicyTransitionFailedAttemptsIgnoreException(StoragePolicyTransitionParamsDto storagePolicyTransitionParamsDto)
    {
        updateStoragePolicyTransitionFailedAttemptsIgnoreExceptionImpl(storagePolicyTransitionParamsDto);
    }
}
