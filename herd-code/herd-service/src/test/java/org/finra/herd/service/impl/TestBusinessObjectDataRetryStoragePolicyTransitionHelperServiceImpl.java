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

import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectDataRetryStoragePolicyTransitionRequest;
import org.finra.herd.model.dto.BusinessObjectDataRetryStoragePolicyTransitionDto;

/**
 * An implementation of the helper service class for the business object data retry storage policy transition functionality for testing.
 */
@Service
@Primary
public class TestBusinessObjectDataRetryStoragePolicyTransitionHelperServiceImpl extends BusinessObjectDataRetryStoragePolicyTransitionHelperServiceImpl
{
    /**
     * {@inheritDoc}
     * <p/>
     * This implementation keeps the current transaction context.
     */
    @Override
    public void executeAwsSpecificSteps(BusinessObjectDataRetryStoragePolicyTransitionDto businessObjectDataRetryStoragePolicyTransitionDto)
    {
        executeAwsSpecificStepsImpl(businessObjectDataRetryStoragePolicyTransitionDto);
    }

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation keeps the current transaction context.
     */
    @Override
    public BusinessObjectData executeRetryStoragePolicyTransitionAfterStep(
        BusinessObjectDataRetryStoragePolicyTransitionDto businessObjectDataRetryStoragePolicyTransitionDto)
    {
        return executeRetryStoragePolicyTransitionAfterStepImpl(businessObjectDataRetryStoragePolicyTransitionDto);
    }

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation keeps the current transaction context.
     */
    @Override
    public BusinessObjectDataRetryStoragePolicyTransitionDto prepareToRetryStoragePolicyTransition(BusinessObjectDataKey businessObjectDataKey,
        BusinessObjectDataRetryStoragePolicyTransitionRequest request)
    {
        return prepareToRetryStoragePolicyTransitionImpl(businessObjectDataKey, request);
    }
}
