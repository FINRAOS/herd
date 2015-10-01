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

import com.amazonaws.services.kms.AWSKMSClient;
import com.amazonaws.services.kms.model.DecryptRequest;
import com.amazonaws.services.kms.model.DecryptResult;

/**
 * AWS KMS Operations Service.
 */
public interface KmsOperations
{
    /**
     * Executes the decrypt request by calling the AWS KMS service.
     *
     * @param awsKmsClient the client for accessing the AWS KMS service
     * @param decryptRequest the decrypt request
     *
     * @return the response from the decrypt service method, as returned by AWS KMS service
     */
    public DecryptResult decrypt(AWSKMSClient awsKmsClient, DecryptRequest decryptRequest);
}
