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

import org.finra.herd.model.api.xml.EmrClusterCreateRequest;
import org.finra.herd.model.api.xml.EmrClusterDefinition;
import org.finra.herd.model.dto.EmrClusterAlternateKeyDto;
import org.finra.herd.model.dto.EmrClusterCreateDto;

/**
 * An implementation of the helper service class for the business object data initiate destroy request functionality for testing.
 */
@Service
@Primary
public class TestEmrHelperServiceImpl extends EmrHelperServiceImpl
{
    /**
     * {@inheritDoc}
     * <p/>
     * This implementation keeps the current transaction context.
     */
    @Override
    public EmrClusterCreateDto emrCreateClusterAwsSpecificSteps(EmrClusterCreateRequest request, EmrClusterDefinition emrClusterDefinition,
        EmrClusterAlternateKeyDto emrClusterAlternateKeyDto)
    {
        return emrCreateClusterAwsSpecificStepsImpl(request, emrClusterDefinition, emrClusterAlternateKeyDto);
    }

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation keeps the current transaction context.
     */
    @Override
    public EmrClusterDefinition emrPreCreateClusterSteps(EmrClusterAlternateKeyDto emrClusterAlternateKeyDto, EmrClusterCreateRequest request) throws Exception
    {
        return emrPreCreateClusterStepsImpl(emrClusterAlternateKeyDto, request);
    }

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation keeps the current transaction context.
     */
    @Override
    public void logEmrClusterCreation(EmrClusterAlternateKeyDto emrClusterAlternateKeyDto, EmrClusterDefinition emrClusterDefinition, String clusterId)
        throws Exception
    {
        logEmrClusterCreationImpl(emrClusterAlternateKeyDto, emrClusterDefinition, clusterId);
    }
}
