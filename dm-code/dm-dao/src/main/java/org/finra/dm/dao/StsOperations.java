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
package org.finra.dm.dao;

import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient;
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import com.amazonaws.services.securitytoken.model.AssumeRoleResult;

/**
 * AWS STS Operations Service.
 */
public interface StsOperations
{
    /**
     * Returns a set of temporary security credentials (consisting of an access key ID, a secret access key, and a security token) that can be used to access
     * the specified AWS resource.
     *
     * @param awsSecurityTokenServiceClient the client for accessing the AWS Security Token Service
     * @param assumeRoleRequest the assume role request
     *
     * @return the response from the AssumeRole service method, as returned by AWS Security Token Service
     */
    public AssumeRoleResult assumeRole(AWSSecurityTokenServiceClient awsSecurityTokenServiceClient, AssumeRoleRequest assumeRoleRequest);
}
