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
package org.finra.herd.app.security;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Set;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.web.authentication.preauth.PreAuthenticatedAuthenticationToken;

import org.finra.herd.app.AbstractAppTest;
import org.finra.herd.model.jpa.SecurityFunctionEntity;

/**
 * This class tests the security helper class.
 */
public class SecurityHelperTest extends AbstractAppTest
{
    @Autowired
    private SecurityHelper securityHelper;

    @Test
    public void testGetUnrestrictedFunctions()
    {
        // Create a security function not mapped to any of the security roles.
        SecurityFunctionEntity securityFunctionEntity = new SecurityFunctionEntity();
        securityFunctionEntity.setCode(SECURITY_FUNCTION);
        herdDao.saveAndRefresh(securityFunctionEntity);

        // Get unrestricted functions.
        Set<GrantedAuthority> result = securityHelper.getUnrestrictedFunctions();

        // Validate that result list contains the test security function.
        assertTrue(result.contains(new SimpleGrantedAuthority(SECURITY_FUNCTION)));
    }

    @Test
    public void testIsGeneratedBy() throws Exception
    {
        assertFalse(securityHelper.isUserGeneratedByClass(null, null));

        PreAuthenticatedAuthenticationToken authRequest = new PreAuthenticatedAuthenticationToken(null, null);
        assertFalse(securityHelper.isUserGeneratedByClass(authRequest, null));
    }
}
