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

import org.finra.herd.model.api.xml.SecurityRoleKey;

public class SecurityRoleHelperTest
{
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Mock
    private AlternateKeyHelper alternateKeyHelper;

    @InjectMocks
    private SecurityRoleHelper securityRoleHelper;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testValidateAndTrimSecurityRoleKey()
    {
        // Create a security role key.
        SecurityRoleKey securityRoleKey = new SecurityRoleKey(SECURITY_ROLE);

        // Mock the external calls.
        when(alternateKeyHelper.validateStringParameter("security role name", SECURITY_ROLE)).thenReturn(SECURITY_ROLE_2);

        // Call the method under test.
        securityRoleHelper.validateAndTrimSecurityRoleKey(securityRoleKey);

        // Validate the results.
        assertEquals(new SecurityRoleKey(SECURITY_ROLE_2), securityRoleKey);

        // Verify the external calls.
        verify(alternateKeyHelper).validateStringParameter("security role name", SECURITY_ROLE);
        verifyNoMoreInteractions(alternateKeyHelper);
    }

    @Test
    public void testValidateAndTrimSecurityRoleKeyMissingSecurityRoleKey()
    {
        // Specify the expected exception.
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("A security role key must be specified.");

        // Call the method under test.
        securityRoleHelper.validateAndTrimSecurityRoleKey(null);

        // Verify the external calls.
        verifyNoMoreInteractions(alternateKeyHelper);
    }
}
