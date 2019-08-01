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
package org.finra.herd.tools.access.validator;

import static org.finra.herd.tools.access.validator.PropertiesHelper.HERD_PASSWORD_PROPERTY;
import static org.finra.herd.tools.access.validator.PropertiesHelper.HERD_USERNAME_PROPERTY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;

/**
 * This class tests functionality within the PropertiesHelper class.
 */
public class PropertiesHelperTest extends AbstractAccessValidatorTest
{
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @InjectMocks
    private PropertiesHelper propertiesHelper;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testLoadAndGetProperties() throws IOException
    {
        // Create a temporary file.
        final File tempFile = temporaryFolder.newFile(PROPERTIES_FILE_PATH);

        // Write several properties including the herdPassword property that is not supposed to get printed to the log.
        FileUtils.writeStringToFile(tempFile, String.format("%s=%s%n%s=%s%n", HERD_USERNAME_PROPERTY, HERD_USERNAME, HERD_PASSWORD_PROPERTY, HERD_PASSWORD),
            StandardCharsets.UTF_8.name());

        // Load the properties.
        propertiesHelper.loadProperties(tempFile);

        // Validate values of the loaded properties.
        assertEquals(HERD_USERNAME, propertiesHelper.getProperty(HERD_USERNAME_PROPERTY));
        assertEquals(HERD_PASSWORD, propertiesHelper.getProperty(HERD_PASSWORD_PROPERTY));

        // Validate that getProperty() method returns null if the property is not found.
        assertNull(propertiesHelper.getProperty(INVALID_PROPERTY));
    }
}
