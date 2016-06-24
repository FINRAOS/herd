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

import java.util.List;

import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Service;

import org.finra.herd.model.dto.StorageUnitAlternateKeyDto;

/**
 * An implementation of the business object data finalize restore service for testing.
 */
@Service
@Primary
public class TestBusinessObjectDataFinalizeRestoreServiceImpl extends BusinessObjectDataFinalizeRestoreServiceImpl
{
    /**
     * {@inheritDoc}
     * <p/>
     * This implementation keeps the current transaction context.
     */
    @Override
    public List<StorageUnitAlternateKeyDto> getGlacierStorageUnitsToRestore(int maxResult)
    {
        return getGlacierStorageUnitsToRestoreImpl(maxResult);
    }

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation keeps the current transaction context.
     */
    @Override
    public void finalizeRestore(StorageUnitAlternateKeyDto glacierStorageUnitKey)
    {
        finalizeRestoreImpl(glacierStorageUnitKey);
    }
}
