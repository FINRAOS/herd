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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import org.finra.herd.dao.AbstractDaoTest;
import org.finra.herd.dao.impl.MockKmsOperationsImpl;
import org.finra.herd.model.dto.ConfigurationValue;

/**
 * This class tests functionality within the KmsHelper class.
 */
public class KmsHelperTest extends AbstractDaoTest
{
    @Autowired
    private KmsHelper kmsHelper;

    @Test
    public void testGetDecryptedConfigurationValue() throws Exception
    {
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.ACTIVITI_JOB_DEFINITION_ID_TEMPLATE.getKey(), MockKmsOperationsImpl.MOCK_CIPHER_TEXT);
        modifyPropertySourceInEnvironment(overrideMap);

        try
        {
            String resultDecryptedValue = kmsHelper.getDecryptedConfigurationValue(ConfigurationValue.ACTIVITI_JOB_DEFINITION_ID_TEMPLATE);
            assertEquals(MockKmsOperationsImpl.MOCK_PLAIN_TEXT, resultDecryptedValue);
        }
        finally
        {
            restorePropertySourceInEnvironment();
        }
    }

    @Test
    public void testGetDecryptedConfigurationValueBlank() throws Exception
    {
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.ACTIVITI_JOB_DEFINITION_ID_TEMPLATE.getKey(), BLANK_TEXT);
        modifyPropertySourceInEnvironment(overrideMap);

        try
        {
            kmsHelper.getDecryptedConfigurationValue(ConfigurationValue.ACTIVITI_JOB_DEFINITION_ID_TEMPLATE);
            fail("Suppose to throw an IllegalStateException when encrypted configuration value is blank.");
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("Unable to decrypt configuration value \"%s\" since it is not configured.",
                ConfigurationValue.ACTIVITI_JOB_DEFINITION_ID_TEMPLATE.getKey()), e.getMessage());
        }
        finally
        {
            restorePropertySourceInEnvironment();
        }
    }

    @Test
    public void testGetDecryptedConfigurationValueDecryptionError() throws Exception
    {
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.ACTIVITI_JOB_DEFINITION_ID_TEMPLATE.getKey(), MockKmsOperationsImpl.MOCK_CIPHER_TEXT_INVALID);
        modifyPropertySourceInEnvironment(overrideMap);

        try
        {
            kmsHelper.getDecryptedConfigurationValue(ConfigurationValue.ACTIVITI_JOB_DEFINITION_ID_TEMPLATE);
            fail("Suppose to throw an IllegalStateException when encrypted value is invalid.");
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("Error decrypting configuration value \"%s\".", ConfigurationValue.ACTIVITI_JOB_DEFINITION_ID_TEMPLATE.getKey()),
                e.getMessage());
        }
        finally
        {
            restorePropertySourceInEnvironment();
        }
    }
}
