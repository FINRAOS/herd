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
package org.finra.herd.service.helper;

import static org.finra.herd.dao.AbstractDaoTest.SECURITY_FUNCTION;
import static org.finra.herd.dao.AbstractDaoTest.SECURITY_FUNCTION_2;
import static org.finra.herd.dao.AbstractDaoTest.SECURITY_ROLE;
import static org.finra.herd.dao.AbstractDaoTest.SECURITY_ROLE_2;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.model.api.xml.SecurityRoleFunctionCreateRequest;
import org.finra.herd.model.api.xml.SecurityRoleFunctionKey;

public class SecurityRoleFunctionHelperTest
{
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Mock
    private AlternateKeyHelper alternateKeyHelper;

    @InjectMocks
    private SecurityRoleFunctionHelper securityRoleFunctionHelper;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testValidateAndTrimSecurityRoleFunctionCreateRequest()
    {
        // Create a security role to function mapping key.
        SecurityRoleFunctionKey securityRoleFunctionKey = new SecurityRoleFunctionKey(SECURITY_ROLE, SECURITY_FUNCTION);

        // Create a security role to function mapping create request.
        SecurityRoleFunctionCreateRequest securityRoleFunctionCreateRequest = new SecurityRoleFunctionCreateRequest(securityRoleFunctionKey);

        // Mock the external calls.
        when(alternateKeyHelper.validateStringParameter("security role name", SECURITY_ROLE)).thenReturn(SECURITY_ROLE_2);
        when(alternateKeyHelper.validateStringParameter("security function name", SECURITY_FUNCTION)).thenReturn(SECURITY_FUNCTION_2);

        // Call the method under test.
        securityRoleFunctionHelper.validateAndTrimSecurityRoleFunctionCreateRequest(securityRoleFunctionCreateRequest);

        // Validate the results.
        assertEquals(new SecurityRoleFunctionCreateRequest(new SecurityRoleFunctionKey(SECURITY_ROLE_2, SECURITY_FUNCTION_2)),
            securityRoleFunctionCreateRequest);

        // Verify the external calls.
        verify(alternateKeyHelper).validateStringParameter("security role name", SECURITY_ROLE);
        verify(alternateKeyHelper).validateStringParameter("security function name", SECURITY_FUNCTION);
        verifyNoMoreInteractions(alternateKeyHelper);
    }

    @Test
    public void testValidateAndTrimSecurityRoleFunctionCreateRequestMissingSecurityRoleFunctionCreateRequest()
    {
        // Specify the expected exception.
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("A security role to function mapping create request must be specified.");

        // Call the method under test.
        securityRoleFunctionHelper.validateAndTrimSecurityRoleFunctionCreateRequest(null);

        // Verify the external calls.
        verifyNoMoreInteractions(alternateKeyHelper);
    }

    @Test
    public void testValidateAndTrimSecurityRoleFunctionCreateRequestMissingSecurityRoleFunctionKey()
    {
        // Specify the expected exception.
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("A security role to function mapping key must be specified.");

        // Call the method under test.
        securityRoleFunctionHelper.validateAndTrimSecurityRoleFunctionCreateRequest(new SecurityRoleFunctionCreateRequest());

        // Verify the external calls.
        verifyNoMoreInteractions(alternateKeyHelper);
    }

    @Test
    public void testValidateAndTrimSecurityRoleFunctionKey()
    {
        // Create a security role to function mapping key.
        SecurityRoleFunctionKey securityRoleFunctionKey = new SecurityRoleFunctionKey(SECURITY_ROLE, SECURITY_FUNCTION);

        // Mock the external calls.
        when(alternateKeyHelper.validateStringParameter("security role name", SECURITY_ROLE)).thenReturn(SECURITY_ROLE_2);
        when(alternateKeyHelper.validateStringParameter("security function name", SECURITY_FUNCTION)).thenReturn(SECURITY_FUNCTION_2);

        // Call the method under test.
        securityRoleFunctionHelper.validateAndTrimSecurityRoleFunctionKey(securityRoleFunctionKey);

        // Validate the results.
        assertEquals(new SecurityRoleFunctionKey(SECURITY_ROLE_2, SECURITY_FUNCTION_2), securityRoleFunctionKey);

        // Verify the external calls.
        verify(alternateKeyHelper).validateStringParameter("security role name", SECURITY_ROLE);
        verify(alternateKeyHelper).validateStringParameter("security function name", SECURITY_FUNCTION);
        verifyNoMoreInteractions(alternateKeyHelper);
    }

    @Test
    public void testValidateAndTrimSecurityRoleFunctionKeyMissingSecurityRoleFunctionKey()
    {
        // Specify the expected exception.
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("A security role to function mapping key must be specified.");

        // Call the method under test.
        securityRoleFunctionHelper.validateAndTrimSecurityRoleFunctionKey(null);

        // Verify the external calls.
        verifyNoMoreInteractions(alternateKeyHelper);
    }
}
