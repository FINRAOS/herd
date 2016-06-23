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
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections4.IterableUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.core.helper.WildcardHelper;
import org.finra.herd.dao.NamespaceDao;
import org.finra.herd.dao.UserDao;
import org.finra.herd.dao.UserNamespaceAuthorizationDao;
import org.finra.herd.model.api.xml.NamespaceAuthorization;
import org.finra.herd.model.dto.ApplicationUser;
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.model.jpa.UserNamespaceAuthorizationEntity;

public class UserNamespaceAuthorizationHelperTest
{
    @InjectMocks
    private UserNamespaceAuthorizationHelper userNamespaceAuthorizationHelper;

    @Mock
    private NamespaceDao namespaceDao;

    @Mock
    private ConfigurationHelper configurationHelper;

    @Mock
    private UserDao userDao;

    @Mock
    private UserNamespaceAuthorizationDao userNamespaceAuthorizationDao;

    @Mock
    private WildcardHelper wildcardHelper;

    @Before
    public void before()
    {
        initMocks(this);
    }

    @Test
    public void testBuildNamespaceAuthorizationsAssertAuthLookupByUserId()
    {
        ApplicationUser applicationUser = new ApplicationUser(getClass());
        String userId = "userId";
        applicationUser.setUserId(userId);

        when(configurationHelper.getBooleanProperty(any())).thenReturn(true);

        List<UserNamespaceAuthorizationEntity> userNamespaceAuthorizationEntities = new ArrayList<>();
        UserNamespaceAuthorizationEntity userNamespaceAuthorizationEntity = new UserNamespaceAuthorizationEntity();
        userNamespaceAuthorizationEntity.setUserId("userNamespaceAuthorizationEntityUserId");
        NamespaceEntity namespaceEntity = new NamespaceEntity();
        namespaceEntity.setCode("namespace");
        userNamespaceAuthorizationEntity.setNamespace(namespaceEntity);
        userNamespaceAuthorizationEntities.add(userNamespaceAuthorizationEntity);
        when(userNamespaceAuthorizationDao.getUserNamespaceAuthorizationsByUserId(any())).thenReturn(userNamespaceAuthorizationEntities);

        userNamespaceAuthorizationHelper.buildNamespaceAuthorizations(applicationUser);

        assertEquals(1, applicationUser.getNamespaceAuthorizations().size());
        NamespaceAuthorization namespaceAuthorization = IterableUtils.get(applicationUser.getNamespaceAuthorizations(), 0);
        assertEquals(namespaceEntity.getCode(), namespaceAuthorization.getNamespace());

        verify(userNamespaceAuthorizationDao).getUserNamespaceAuthorizationsByUserId(eq(userId));
        verify(userNamespaceAuthorizationDao).getUserNamespaceAuthorizationsByUserIdStartsWith(eq(WildcardHelper.WILDCARD_TOKEN));
        verifyNoMoreInteractions(userNamespaceAuthorizationDao, wildcardHelper);
    }

    @Test
    public void testBuildNamespaceAuthorizationsAssertWildcardQueryExecuted()
    {
        ApplicationUser applicationUser = new ApplicationUser(getClass());
        String userId = "userId";
        applicationUser.setUserId(userId);

        when(configurationHelper.getBooleanProperty(any())).thenReturn(true);

        List<UserNamespaceAuthorizationEntity> wildcardEntities = new ArrayList<>();
        UserNamespaceAuthorizationEntity wildcardEntity = new UserNamespaceAuthorizationEntity();
        wildcardEntity.setUserId("wildcardEntityUserId");
        NamespaceEntity namespaceEntity = new NamespaceEntity();
        namespaceEntity.setCode("namespace");
        wildcardEntity.setNamespace(namespaceEntity);
        wildcardEntities.add(wildcardEntity);
        when(userNamespaceAuthorizationDao.getUserNamespaceAuthorizationsByUserIdStartsWith(any())).thenReturn(wildcardEntities);

        when(wildcardHelper.matches(any(), any())).thenReturn(true);

        userNamespaceAuthorizationHelper.buildNamespaceAuthorizations(applicationUser);

        assertEquals(1, applicationUser.getNamespaceAuthorizations().size());
        NamespaceAuthorization namespaceAuthorization = IterableUtils.get(applicationUser.getNamespaceAuthorizations(), 0);
        assertEquals(namespaceEntity.getCode(), namespaceAuthorization.getNamespace());

        verify(userNamespaceAuthorizationDao).getUserNamespaceAuthorizationsByUserId(eq(userId));
        verify(userNamespaceAuthorizationDao).getUserNamespaceAuthorizationsByUserIdStartsWith(eq(WildcardHelper.WILDCARD_TOKEN));
        verify(wildcardHelper).matches(eq(userId.toUpperCase()), eq(wildcardEntity.getUserId().toUpperCase()));
        verifyNoMoreInteractions(userNamespaceAuthorizationDao, wildcardHelper);
    }

    @Test
    public void testBuildNamespaceAuthorizationsAssertWildcardEntityNotAddedIfMatchFails()
    {
        ApplicationUser applicationUser = new ApplicationUser(getClass());
        String userId = "userId";
        applicationUser.setUserId(userId);

        when(configurationHelper.getBooleanProperty(any())).thenReturn(true);

        List<UserNamespaceAuthorizationEntity> wildcardEntities = new ArrayList<>();
        UserNamespaceAuthorizationEntity wildcardEntity = new UserNamespaceAuthorizationEntity();
        wildcardEntity.setUserId("wildcardEntityUserId");
        NamespaceEntity namespaceEntity = new NamespaceEntity();
        namespaceEntity.setCode("namespace");
        wildcardEntity.setNamespace(namespaceEntity);
        wildcardEntities.add(wildcardEntity);
        when(userNamespaceAuthorizationDao.getUserNamespaceAuthorizationsByUserIdStartsWith(any())).thenReturn(wildcardEntities);

        when(wildcardHelper.matches(any(), any())).thenReturn(false);

        userNamespaceAuthorizationHelper.buildNamespaceAuthorizations(applicationUser);

        assertEquals(0, applicationUser.getNamespaceAuthorizations().size());

        verify(userNamespaceAuthorizationDao).getUserNamespaceAuthorizationsByUserId(eq(userId));
        verify(userNamespaceAuthorizationDao).getUserNamespaceAuthorizationsByUserIdStartsWith(eq(WildcardHelper.WILDCARD_TOKEN));
        verify(wildcardHelper).matches(eq(userId.toUpperCase()), eq(wildcardEntity.getUserId().toUpperCase()));
        verifyNoMoreInteractions(userNamespaceAuthorizationDao, wildcardHelper);
    }
}
