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

import org.finra.herd.model.api.xml.EmrClusterCreateRequest;
import org.finra.herd.model.api.xml.EmrClusterDefinition;
import org.finra.herd.model.dto.EmrClusterAlternateKeyDto;
import org.finra.herd.model.dto.EmrClusterCreateDto;

/**
 * The helper service class for the emr functionality.
 */
public interface EmrHelperService
{
    /**
     * Executes steps required to prepare for the EMR cluster creation.
     *
     * @param emrClusterAlternateKeyDto the ERM cluster alternate key
     * @param request The EMR cluster create request
     *
     * @return the EMR cluster definition
     * @throws Exception Exception when the original EMR cluster definition XML is malformed
     */
    EmrClusterDefinition emrPreCreateClusterSteps(EmrClusterAlternateKeyDto emrClusterAlternateKeyDto, EmrClusterCreateRequest request) throws Exception;

    /**
     * Executes S3 specific steps for EMR cluster creation.
     *
     * @param request The EMR cluster create request
     * @param emrClusterDefinition the EMR cluster definition
     * @param emrClusterAlternateKeyDto the ERM cluster alternate key
     *
     * @return an EMR cluster create DTO with the cluster creation information
     */
    EmrClusterCreateDto emrCreateClusterAwsSpecificSteps(EmrClusterCreateRequest request, EmrClusterDefinition emrClusterDefinition,
        EmrClusterAlternateKeyDto emrClusterAlternateKeyDto);

    /**
     * Logs the EMR cluster creation event.
     *
     * @param emrClusterAlternateKeyDto the ERM cluster alternate key
     * @param emrClusterDefinition the EMR cluster definition
     * @param clusterId the EMR cluster Id
     *
     * @throws Exception Exception can occur when converting the EMR cluster definition to XML
     */
    void logEmrClusterCreation(EmrClusterAlternateKeyDto emrClusterAlternateKeyDto, EmrClusterDefinition emrClusterDefinition, String clusterId)
        throws Exception;
}
