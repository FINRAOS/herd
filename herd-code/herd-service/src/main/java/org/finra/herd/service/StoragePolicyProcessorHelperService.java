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
package org.finra.herd.service;

import org.finra.herd.model.dto.StoragePolicySelection;
import org.finra.herd.model.dto.StoragePolicyTransitionParamsDto;

/**
 * The helper service class for the storage policy processor service.
 */
public interface StoragePolicyProcessorHelperService
{
    /**
     * Initiates a storage policy transition as per specified storage policy selection. This method also updates storage policy transition DTO with parameters
     * needed to perform a storage policy transition.
     *
     * @param storagePolicyTransitionParamsDto the storage policy transition DTO to be updated with parameters needed to perform a storage policy transition
     * @param storagePolicySelection the storage policy selection message
     */
    public void initiateStoragePolicyTransition(StoragePolicyTransitionParamsDto storagePolicyTransitionParamsDto,
        StoragePolicySelection storagePolicySelection);

    /**
     * Executes a storage policy transition as per specified storage policy selection.
     *
     * @param storagePolicyTransitionParamsDto the storage policy transition DTO that contains parameters needed to perform a storage policy transition
     */
    public void executeStoragePolicyTransition(StoragePolicyTransitionParamsDto storagePolicyTransitionParamsDto);

    /**
     * Completes a storage policy transition as per specified storage policy selection.
     *
     * @param storagePolicyTransitionParamsDto the storage policy transition DTO that contains parameters needed to complete a storage policy transition
     */
    public void completeStoragePolicyTransition(StoragePolicyTransitionParamsDto storagePolicyTransitionParamsDto);

    /**
     * Increments the count for failed storage policy transition attempts for the specified storage unit. This method does not fail in case storage unit entity
     * update is unsuccessful, but simply logs the exception information as a warning.
     *
     * @param storagePolicyTransitionParamsDto the storage policy transition DTO that contains parameters needed to complete a storage policy transition. The
     * business object data key and storage name identify the storage unit to be updated
     * @param ignoreException the storage policy transition exception that will be ignored
     */
    public void updateStoragePolicyTransitionFailedAttemptsIgnoreException(StoragePolicyTransitionParamsDto storagePolicyTransitionParamsDto,
        RuntimeException ignoreException);
}
