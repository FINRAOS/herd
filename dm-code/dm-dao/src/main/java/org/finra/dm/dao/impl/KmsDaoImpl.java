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
package org.finra.dm.dao.impl;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import com.amazonaws.services.kms.AWSKMSClient;
import com.amazonaws.services.kms.model.DecryptRequest;
import com.amazonaws.services.kms.model.DecryptResult;
import org.apache.commons.codec.binary.Base64;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import org.finra.dm.dao.KmsDao;
import org.finra.dm.dao.KmsOperations;
import org.finra.dm.dao.helper.AwsHelper;
import org.finra.dm.model.dto.AwsParamsDto;

/**
 * The AWS KMS DAO implementation.
 */
@Repository
public class KmsDaoImpl implements KmsDao
{
    @Autowired
    private AwsHelper awsHelper;

    @Autowired
    private KmsOperations kmsOperations;

    /**
     * {@inheritDoc}
     */
    @Override
    public String decrypt(AwsParamsDto awsParamsDto, String base64ciphertextBlob)
    {
        // Construct a new AWS KMS service client using the specified client configuration.
        // A credentials provider chain will be used that searches for credentials in this order:
        // - Environment Variables - AWS_ACCESS_KEY_ID and AWS_SECRET_KEY
        // - Java System Properties - aws.accessKeyId and aws.secretKey
        // - Instance Profile Credentials - delivered through the Amazon EC2 metadata service
        AWSKMSClient awsKmsClient = new AWSKMSClient(awsHelper.getClientConfiguration(awsParamsDto));

        // Decode the base64 encoded ciphertext.
        ByteBuffer ciphertextBlob = ByteBuffer.wrap(Base64.decodeBase64(base64ciphertextBlob));

        // Create the decrypt request.
        DecryptRequest decryptRequest = new DecryptRequest().withCiphertextBlob(ciphertextBlob);

        // Call AWS KMS decrypt service method.
        DecryptResult decryptResult = kmsOperations.decrypt(awsKmsClient, decryptRequest);

        // Get decrypted plaintext data.
        ByteBuffer plainText = decryptResult.getPlaintext();

        // Return the plain text as a string.
        return new String(plainText.array(), StandardCharsets.UTF_8);
    }
}
