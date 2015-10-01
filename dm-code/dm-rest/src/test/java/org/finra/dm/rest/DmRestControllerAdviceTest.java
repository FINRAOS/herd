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
package org.finra.dm.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;

import org.finra.dm.core.Command;
import org.finra.dm.model.MethodNotAllowedException;
import org.finra.dm.model.dto.ConfigurationValue;
import org.finra.dm.model.jpa.ConfigurationEntity;
import org.finra.dm.model.api.xml.ErrorInformation;
import org.finra.dm.service.helper.DmErrorInformationExceptionHandler;

/**
 * Test the DM Rest Controller Advice. There is very little functionality to test since the majority of functionality has been moved into the base class and is
 * tested in a different JUnit for that class. The only thing specific to the REST controller advice is that logging should be enabled.
 */
public class DmRestControllerAdviceTest extends AbstractRestTest
{
    @Autowired
    private DmRestControllerAdvice restControllerAdvice;

    @Test
    public void testIsLoggingEnabled() throws Exception
    {
        assertTrue(restControllerAdvice.isLoggingEnabled());
    }

    @Test
    public void testHandleInternalServerError() throws Exception
    {
        executeWithoutLogging(DmErrorInformationExceptionHandler.class, new Command()
        {
            @Override
            public void execute()
            {
                // Perform a basic test to ensure the advice works. This and all other tests are handled in the JUnit for the base class.
                // This will normally log an error, but it's turned off above.
                ErrorInformation errorInformation = restControllerAdvice.handleInternalServerErrorException(new Exception("test"));
                assertTrue(errorInformation.getStatusCode() == HttpStatus.INTERNAL_SERVER_ERROR.value());
            }
        });
    }

    @Test
    public void testCheckNotAllowedMethod()
    {
        // Create the not allowed key
        ConfigurationEntity configurationEntity = dmDao.getConfigurationByKey(ConfigurationValue.NOT_ALLOWED_DM_ENDPOINTS.getKey());
        if (configurationEntity == null)
        {
            configurationEntity = new ConfigurationEntity();
            configurationEntity.setKey(ConfigurationValue.NOT_ALLOWED_DM_ENDPOINTS.getKey());
        }
        configurationEntity.setValue("");
        configurationEntity.setValueClob("org.finra.dm.rest.DmRestController.getBuildInfo");

        dmDao.saveAndRefresh(configurationEntity);

        try
        {
            dmRestController.getBuildInfo();
            fail("Should have thrown a MethodNotAllowedException.");
        }
        catch (MethodNotAllowedException ex)
        {
            assertEquals("The requested method is not allowed.", ex.getMessage());
        }
    }
}
