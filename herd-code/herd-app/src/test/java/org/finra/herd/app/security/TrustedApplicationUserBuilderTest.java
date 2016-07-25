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

import static org.junit.Assert.assertEquals;

import java.util.LinkedHashSet;
import java.util.Set;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.mock.web.MockHttpServletRequest;

import org.finra.herd.app.AbstractAppTest;
import org.finra.herd.model.api.xml.NamespaceAuthorization;
import org.finra.herd.model.dto.ApplicationUser;

/**
 * This class tests the trusted user builder.
 */
public class TrustedApplicationUserBuilderTest extends AbstractAppTest
{
    @Autowired
    TrustedApplicationUserBuilder trustedApplicationUserBuilder;

    @Test
    public void testTrustedUserBuilderNoRoles() throws Exception
    {
        // Create a set of test namespace authorizations.
        Set<NamespaceAuthorization> namespaceAuthorizations = new LinkedHashSet<>();
        namespaceAuthorizations.add(new NamespaceAuthorization(NAMESPACE, SUPPORTED_NAMESPACE_PERMISSIONS));
        namespaceAuthorizations.add(new NamespaceAuthorization(NAMESPACE_2, SUPPORTED_NAMESPACE_PERMISSIONS));

        // Create and persist the relative database entities.
        userNamespaceAuthorizationDaoTestHelper
            .createUserNamespaceAuthorizationEntity(TrustedApplicationUserBuilder.TRUSTED_USER_ID, namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE),
                SUPPORTED_NAMESPACE_PERMISSIONS);
        userNamespaceAuthorizationDaoTestHelper
            .createUserNamespaceAuthorizationEntity(TrustedApplicationUserBuilder.TRUSTED_USER_ID, namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE_2),
                SUPPORTED_NAMESPACE_PERMISSIONS);

        // Build the trusted user.
        ApplicationUser applicationUser = trustedApplicationUserBuilder.buildNoRoles(new MockHttpServletRequest());

        // Validate the trusted user.
        assertEquals(TrustedApplicationUserBuilder.TRUSTED_USER_ID, applicationUser.getUserId());
        assertEquals(TrustedApplicationUserBuilder.TRUSTED_USER_FIRST_NAME, applicationUser.getFirstName());
        assertEquals(TrustedApplicationUserBuilder.TRUSTED_USER_LAST_NAME, applicationUser.getLastName());
        assertEquals(TrustedApplicationUserBuilder.TRUSTED_USER_EMAIL, applicationUser.getEmail());
        assertEquals(namespaceAuthorizations, applicationUser.getNamespaceAuthorizations());
        assertEquals(0, applicationUser.getRoles().size());
    }
}
