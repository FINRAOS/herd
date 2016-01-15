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
package org.finra.herd.dao;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.amazonaws.services.kms.model.InvalidCiphertextException;
import org.junit.Test;

import org.finra.herd.dao.impl.MockKmsOperationsImpl;
import org.finra.herd.model.dto.AwsParamsDto;

/**
 * This class tests the functionality of KmsDao.
 */
public class KmsDaoTest extends AbstractDaoTest
{
    @Test
    public void testDecrypt()
    {
        // Decrypt the test ciphertext.
        AwsParamsDto testAwsParamsDto = new AwsParamsDto();
        testAwsParamsDto.setHttpProxyHost(HTTP_PROXY_HOST);
        testAwsParamsDto.setHttpProxyPort(HTTP_PROXY_PORT);

        // Decrypt the test ciphertext.
        String resultPlainText = kmsDao.decrypt(testAwsParamsDto, MockKmsOperationsImpl.MOCK_CIPHER_TEXT);

        // Validate the results.
        assertEquals(MockKmsOperationsImpl.MOCK_PLAIN_TEXT, resultPlainText);
    }

    @Test
    public void testDecryptInvalidCipher()
    {
        try
        {
            // Try to decrypt an invalid ciphertext.
            kmsDao.decrypt(new AwsParamsDto(), MockKmsOperationsImpl.MOCK_CIPHER_TEXT_INVALID);
            fail("Suppose to throw an InvalidCiphertextException when cipher text is invalid.");
        }
        catch (Exception e)
        {
            assertEquals(InvalidCiphertextException.class, e.getClass());
        }
    }
}
