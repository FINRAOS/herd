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
package org.finra.herd.dao.impl;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.policy.Policy;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient;
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import com.amazonaws.services.securitytoken.model.AssumeRoleResult;
import com.amazonaws.services.securitytoken.model.Credentials;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import org.finra.herd.dao.RetryPolicyFactory;
import org.finra.herd.dao.StsDao;
import org.finra.herd.dao.StsOperations;
import org.finra.herd.model.dto.AwsParamsDto;

/**
 * The STS DAO implementation.
 */
@Repository
public class StsDaoImpl implements StsDao
{
    @Autowired
    private StsOperations stsOperations;

    @Autowired
    private RetryPolicyFactory retryPolicyFactory;

    /**
     * Returns a set of temporary security credentials (consisting of an access key ID, a secret access key, and a security token) that can be used to access
     * the specified AWS resource.
     *
     * @param sessionName the session name that will be associated with the temporary credentials. The session name must be the same for an initial set of
     * credentials and an extended set of credentials if credentials are to be refreshed. The session name also is used to identify the user in AWS logs so it
     * should be something unique and useful to identify the caller/use.
     * @param awsRoleArn the AWS ARN for the role required to provide access to the specified AWS resource
     * @param awsRoleDurationSeconds the duration, in seconds, of the role session. The value can range from 900 seconds (15 minutes) to 3600 seconds (1 hour).
     * @param policy the temporary policy to apply to this request
     *
     * @return the assumed session credentials
     */
    @Override
    public Credentials getTemporarySecurityCredentials(AwsParamsDto awsParamsDto, String sessionName, String awsRoleArn, int awsRoleDurationSeconds,
        Policy policy)
    {
        // Construct a new AWS security token service client using the specified client configuration to access Amazon S3.
        // A credentials provider chain will be used that searches for credentials in this order:
        // - Environment Variables - AWS_ACCESS_KEY_ID and AWS_SECRET_KEY
        // - Java System Properties - aws.accessKeyId and aws.secretKey
        // - Instance Profile Credentials - delivered through the Amazon EC2 metadata service

        ClientConfiguration clientConfiguration = new ClientConfiguration().withRetryPolicy(retryPolicyFactory.getRetryPolicy());

        // Only set the proxy hostname and/or port if they're configured.
        if (StringUtils.isNotBlank(awsParamsDto.getHttpProxyHost()))
        {
            clientConfiguration.setProxyHost(awsParamsDto.getHttpProxyHost());
        }
        if (awsParamsDto.getHttpProxyPort() != null)
        {
            clientConfiguration.setProxyPort(awsParamsDto.getHttpProxyPort());
        }

        AWSSecurityTokenServiceClient awsSecurityTokenServiceClient = new AWSSecurityTokenServiceClient(clientConfiguration);

        // Create the request.
        AssumeRoleRequest assumeRoleRequest = new AssumeRoleRequest();
        assumeRoleRequest.setRoleSessionName(sessionName);
        assumeRoleRequest.setRoleArn(awsRoleArn);
        assumeRoleRequest.setDurationSeconds(awsRoleDurationSeconds);
        assumeRoleRequest.setPolicy(policy.toJson());

        // Get the temporary security credentials.
        AssumeRoleResult assumeRoleResult = stsOperations.assumeRole(awsSecurityTokenServiceClient, assumeRoleRequest);
        return assumeRoleResult.getCredentials();
    }
}
