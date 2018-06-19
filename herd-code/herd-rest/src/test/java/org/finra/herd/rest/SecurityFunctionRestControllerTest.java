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
package org.finra.herd.rest;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.model.api.xml.SecurityFunction;
import org.finra.herd.model.api.xml.SecurityFunctionCreateRequest;
import org.finra.herd.model.api.xml.SecurityFunctionKey;
import org.finra.herd.model.api.xml.SecurityFunctionKeys;
import org.finra.herd.service.SecurityFunctionService;

/**
 * This class tests various functionality within the security function REST controller.
 */
public class SecurityFunctionRestControllerTest extends AbstractRestTest
{
    @InjectMocks
    private SecurityFunctionRestController securityFunctionRestController;

    @Mock
    private SecurityFunctionService securityFunctionService;

    @Before()
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCreateSecurityFunction() throws Exception
    {
        SecurityFunctionCreateRequest securityFunctionCreateRequest = new SecurityFunctionCreateRequest(SECURITY_FUNCTION);
        // Create a security function.
        SecurityFunction securityFunction = new SecurityFunction(SECURITY_FUNCTION);
        when(securityFunctionService.createSecurityFunction(securityFunctionCreateRequest)).thenReturn(securityFunction);

        SecurityFunction resultSecurityFunction = securityFunctionRestController.createSecurityFunction(new SecurityFunctionCreateRequest(SECURITY_FUNCTION));

        // Validate the returned object.
        assertEquals(new SecurityFunction(SECURITY_FUNCTION), resultSecurityFunction);

        // Verify the external calls.
        verify(securityFunctionService).createSecurityFunction(securityFunctionCreateRequest);
        verifyNoMoreInteractions(securityFunctionService);
        // Validate the returned object.
        assertEquals(securityFunction, resultSecurityFunction);
    }

    @Test
    public void testDeleteSecurityFunction() throws Exception
    {
        // Create a security function.
        SecurityFunction securityFunction = new SecurityFunction(SECURITY_FUNCTION);

        when(securityFunctionService.deleteSecurityFunction(new SecurityFunctionKey(SECURITY_FUNCTION))).thenReturn(securityFunction);

        SecurityFunction deletedSecurityFunction = securityFunctionRestController.deleteSecurityFunction(SECURITY_FUNCTION);

        // Verify the external calls.
        verify(securityFunctionService).deleteSecurityFunction(new SecurityFunctionKey(SECURITY_FUNCTION));
        verifyNoMoreInteractions(securityFunctionService);
        // Validate the returned object.
        assertEquals(securityFunction, deletedSecurityFunction);
    }

    @Test
    public void testGetSecurityFunction() throws Exception
    {
        SecurityFunction securityFunction = new SecurityFunction(SECURITY_FUNCTION);
        when(securityFunctionService.getSecurityFunction(new SecurityFunctionKey(SECURITY_FUNCTION))).thenReturn(securityFunction);

        // Retrieve the security function.
        SecurityFunction resultSecurityFunction = securityFunctionRestController.getSecurityFunction(SECURITY_FUNCTION);

        // Verify the external calls.
        verify(securityFunctionService).getSecurityFunction(new SecurityFunctionKey(SECURITY_FUNCTION));
        verifyNoMoreInteractions(securityFunctionService);
        // Validate the returned object.
        assertEquals(securityFunction, resultSecurityFunction);
    }

    @Test
    public void testGetSecurityFunctions() throws Exception
    {
        SecurityFunctionKeys securityFunctionKeys =
            new SecurityFunctionKeys(Arrays.asList(new SecurityFunctionKey(SECURITY_FUNCTION), new SecurityFunctionKey(SECURITY_FUNCTION_2)));

        when(securityFunctionService.getSecurityFunctions()).thenReturn(securityFunctionKeys);

        // Retrieve a list of security function keys.
        SecurityFunctionKeys resultSecurityFunctionKeys = securityFunctionRestController.getSecurityFunctions();

        // Verify the external calls.
        verify(securityFunctionService).getSecurityFunctions();
        verifyNoMoreInteractions(securityFunctionService);
        // Validate the returned object.
        assertEquals(securityFunctionKeys, resultSecurityFunctionKeys);
    }
}
