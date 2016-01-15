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

import java.nio.ByteBuffer;

import com.amazonaws.services.kms.AWSKMSClient;
import com.amazonaws.services.kms.model.DecryptRequest;
import com.amazonaws.services.kms.model.DecryptResult;
import com.amazonaws.services.kms.model.InvalidCiphertextException;
import org.apache.commons.codec.binary.Base64;

import org.finra.herd.dao.KmsOperations;

/**
 * Mock implementation of AWS STS operations.
 */
public class MockKmsOperationsImpl implements KmsOperations
{
    // Create a mock plain text constant and an encrypted version of that string.
    public static final String MOCK_PLAIN_TEXT = "mypassword";
    public static final String MOCK_CIPHER_TEXT = "CiDOchI+rEPGLo7Czt1BKqYqUZ70n5kdBm2L2B1EBvEA8RKRAQEBAg" +
        "B4znISPqxDxi6Ows7dQSqmKlGe9J+ZHQZti9gdRAbxAPEAAABoMGYG" +
        "CSqGSIb3DQEHBqBZMFcCAQAwUgYJKoZIhvcNAQcBMB4GCWCGSAFlAw" +
        "QBLjARBAx6PRIcGMv0JSPFgJgCARCAJdeEAYr20YW0OAnwQccFCfDv" +
        "eLtLPZ3g7jjxbXS/CZJ4BuGZBpY=";

    public static final String MOCK_CIPHER_TEXT_INVALID = "mock_invalid_cipher_text";

    @Override
    public DecryptResult decrypt(AWSKMSClient awsKmsClient, DecryptRequest decryptRequest)
    {
        // Check the cipher text.
        if (decryptRequest.getCiphertextBlob().equals(ByteBuffer.wrap(Base64.decodeBase64(MOCK_CIPHER_TEXT_INVALID))))
        {
            throw new InvalidCiphertextException("(Service: AWSKMS; Status Code: 400; Error Code: InvalidCiphertextException; Request ID: NONE)");
        }

        DecryptResult decryptResult = new DecryptResult();

        // Convert the test plain text to byte buffer and set the plain text return value.
        decryptResult.setPlaintext(ByteBuffer.wrap(MOCK_PLAIN_TEXT.getBytes()));

        return decryptResult;
    }
}
