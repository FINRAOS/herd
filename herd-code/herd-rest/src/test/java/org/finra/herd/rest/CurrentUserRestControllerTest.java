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

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.model.api.xml.NamespaceAuthorization;
import org.finra.herd.model.api.xml.UserAuthorizations;
import org.finra.herd.service.CurrentUserService;

/**
 * This class tests various functionality within the current user REST controller.
 */
public class CurrentUserRestControllerTest extends AbstractRestTest
{
    @InjectMocks
    private CurrentUserRestController currentUserRestController;

    @Mock
    private CurrentUserService currentUserService;

    @Before()
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testGetCurrentUser() throws Exception
    {
        // Create a set of test namespace authorizations.
        Set<NamespaceAuthorization> namespaceAuthorizations = new LinkedHashSet<>();
        namespaceAuthorizations.add(new NamespaceAuthorization(NAMESPACE, SUPPORTED_NAMESPACE_PERMISSIONS));
        namespaceAuthorizations.add(new NamespaceAuthorization(NAMESPACE_2, SUPPORTED_NAMESPACE_PERMISSIONS));

        UserAuthorizations userAuthorizations = new UserAuthorizations();
        userAuthorizations.setNamespaceAuthorizations(new ArrayList(namespaceAuthorizations));

        when(currentUserService.getCurrentUser()).thenReturn(userAuthorizations);
        // Get the current user information.
        UserAuthorizations resultUserAuthorizations = currentUserRestController.getCurrentUser();

        // Verify the external calls.
        verify(currentUserService).getCurrentUser();
        verifyNoMoreInteractions(currentUserService);
        // Validate the returned object.
        assertEquals(userAuthorizations, resultUserAuthorizations);
    }
}
