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
package org.finra.herd.tools.common.databridge;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.finra.herd.model.api.xml.AwsCredential;
import org.finra.herd.model.dto.HerdAWSCredentialsProvider;

/**
 * An abstract AWS credentials provider which automatically refreshes based on the session expiration time.
 */
public abstract class AutoRefreshCredentialProvider implements HerdAWSCredentialsProvider
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AutoRefreshCredentialProvider.class);

    /**
     * The session expiration time in milliseconds for the current AWS credentials.
     */
    private Long sessionExpirationTime;

    /**
     * The current AWS credentials.
     */
    private AwsCredential currentAwsCredentials;

    /**
     * Gets a new set of AWS credentials.
     * 
     * @return AWS credentials
     * @throws Exception when any exception occurs
     */
    public abstract AwsCredential getNewAwsCredential() throws Exception;

    /**
     * Retrieves a fresh set of credentials if there is no current cached credentials, or the session has expired. Otherwise, returns the cached credentials.
     */
    @Override
    public AwsCredential getAwsCredential()
    {
        if (sessionExpirationTime == null || System.currentTimeMillis() >= sessionExpirationTime)
        {
            try
            {
                currentAwsCredentials = getNewAwsCredential();
            }
            catch (Exception e)
            {
                LOGGER.warn("Error retrieving new credentials", e);
                throw new IllegalStateException(e);
            }
            sessionExpirationTime = currentAwsCredentials.getAwsSessionExpirationTime().toGregorianCalendar().getTimeInMillis();
        }
        return currentAwsCredentials;
    }
}
