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
package org.finra.herd.dao.helper;

import org.springframework.stereotype.Component;

import org.finra.herd.model.dto.ConfigurationValue;

@Component
public class AwsSqsExceptionRetryAdvice extends AwsExceptionRetryAdvice
{
    @Override
    protected int getMaxRetryDelaySecs()
    {
        return herdStringHelper.getConfigurationValueAsInteger(ConfigurationValue.AWS_SQS_EXCEPTION_MAX_RETRY_DURATION_SECS);
    }

    @Override
    protected String getExceptionErrorCodes()
    {
        return herdStringHelper.getConfigurationValueAsString(ConfigurationValue.AWS_SQS_RETRY_ON_ERROR_CODES);
    }
}
