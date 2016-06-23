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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;

import org.finra.herd.model.api.xml.NamespaceAuthorization;
import org.finra.herd.model.api.xml.NamespacePermissionEnum;
import org.finra.herd.model.dto.ApplicationUser;
import org.finra.herd.model.dto.SecurityUserWrapper;

public class NamespaceSecurityHelperTest
{
    private NamespaceSecurityHelper namespaceSecurityHelper;

    @Before
    public void before()
    {
        namespaceSecurityHelper = new NamespaceSecurityHelper();
    }

    @After
    public void after()
    {
        SecurityContextHolder.clearContext();
    }

    @Test
    public void getAuthorizedNamespacesWhenUserHasPermissionAssertReturnNamespace()
    {
        ApplicationUser applicationUser = new ApplicationUser(getClass());
        applicationUser
            .setNamespaceAuthorizations(new HashSet<>(Arrays.asList(new NamespaceAuthorization("namespace", Arrays.asList(NamespacePermissionEnum.READ)))));
        SecurityContextHolder.getContext().setAuthentication(
            new TestingAuthenticationToken(new SecurityUserWrapper("username", "", true, true, true, true, Collections.emptyList(), applicationUser), null));

        Set<String> authorizedNamespaces = namespaceSecurityHelper.getAuthorizedNamespaces(NamespacePermissionEnum.READ);
        assertEquals(1, authorizedNamespaces.size());
        assertTrue(authorizedNamespaces.contains("namespace"));
    }

    @Test
    public void getAuthorizedNamespacesWhenUserHasNoPermissionAssertReturnEmpty()
    {
        ApplicationUser applicationUser = new ApplicationUser(getClass());
        applicationUser
            .setNamespaceAuthorizations(new HashSet<>(Arrays.asList(new NamespaceAuthorization("namespace", Arrays.asList(NamespacePermissionEnum.WRITE)))));
        SecurityContextHolder.getContext().setAuthentication(
            new TestingAuthenticationToken(new SecurityUserWrapper("username", "", true, true, true, true, Collections.emptyList(), applicationUser), null));

        Set<String> authorizedNamespaces = namespaceSecurityHelper.getAuthorizedNamespaces(NamespacePermissionEnum.READ);
        assertEquals(0, authorizedNamespaces.size());
    }

    @Test
    public void getAuthorizedNamespacesWhenNoApplicationUserInContextReturnEmpty()
    {
        SecurityContextHolder.getContext().setAuthentication(
            new TestingAuthenticationToken(new SecurityUserWrapper("username", "", true, true, true, true, Collections.emptyList(), null), null));

        Set<String> authorizedNamespaces = namespaceSecurityHelper.getAuthorizedNamespaces(NamespacePermissionEnum.READ);
        assertEquals(0, authorizedNamespaces.size());
    }

    @Test
    public void getAuthorizedNamespacesWhenNoAuthenticationInContextReturnEmpty()
    {
        SecurityContextHolder.clearContext();

        Set<String> authorizedNamespaces = namespaceSecurityHelper.getAuthorizedNamespaces(NamespacePermissionEnum.READ);
        assertEquals(0, authorizedNamespaces.size());
    }
}
