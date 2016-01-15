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

import org.finra.herd.model.api.xml.EmrClusterDefinitionCreateRequest;
import org.finra.herd.model.api.xml.EmrClusterDefinitionInformation;
import org.finra.herd.model.api.xml.EmrClusterDefinitionKey;
import org.finra.herd.model.api.xml.EmrClusterDefinitionUpdateRequest;

/**
 * The EMR cluster definition service.
 */
public interface EmrClusterDefinitionService
{
    public EmrClusterDefinitionInformation createEmrClusterDefinition(EmrClusterDefinitionCreateRequest emrClusterDefinitionCreateRequest) throws Exception;

    public EmrClusterDefinitionInformation getEmrClusterDefinition(EmrClusterDefinitionKey emrClusterDefinitionKey) throws Exception;

    public EmrClusterDefinitionInformation updateEmrClusterDefinition(EmrClusterDefinitionKey emrClusterDefinitionKey,
        EmrClusterDefinitionUpdateRequest emrClusterDefinitionUpdateRequest) throws Exception;

    public EmrClusterDefinitionInformation deleteEmrClusterDefinition(EmrClusterDefinitionKey emrClusterDefinitionKey) throws Exception;
}
