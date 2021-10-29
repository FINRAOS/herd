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

import static org.finra.herd.service.AbstractServiceTest.NAMESPACE_CODE;
import static org.finra.herd.service.AbstractServiceTest.USER_ID;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import org.springframework.test.util.ReflectionTestUtils;

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
import org.finra.herd.model.api.xml.NamespaceKey;
import org.finra.herd.model.api.xml.NamespacePermissionEnum;
import org.finra.herd.model.dto.ApplicationUser;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.model.jpa.UserEntity;
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
        ReflectionTestUtils.setField(userNamespaceAuthorizationEntity, "namespaceCode", "namespace");

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
        ReflectionTestUtils.setField(wildcardEntity, "namespaceCode", "namespace");
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

    @Test
    public void testBuildNamespaceAuthorizationsWithUserNamespaceAuthorizationNotEnabled()
    {
        ApplicationUser applicationUser = new ApplicationUser(getClass());
        applicationUser.setUserId(USER_ID);

        List<NamespaceKey> namespaceKeys = new ArrayList<>();
        namespaceKeys.add(new NamespaceKey(NAMESPACE_CODE));

        when(configurationHelper.getBooleanProperty(ConfigurationValue.USER_NAMESPACE_AUTHORIZATION_ENABLED)).thenReturn(false);
        when(namespaceDao.getNamespaces()).thenReturn(namespaceKeys);

        userNamespaceAuthorizationHelper.buildNamespaceAuthorizations(applicationUser);

        List<NamespaceAuthorization> namespaceAuthorizations = new ArrayList<>(applicationUser.getNamespaceAuthorizations());

        assertThat(namespaceAuthorizations.get(0).getNamespacePermissions().contains(NamespacePermissionEnum.EXECUTE), is(true));
        assertThat(namespaceAuthorizations.get(0).getNamespacePermissions().contains(NamespacePermissionEnum.GRANT), is(true));
        assertThat(namespaceAuthorizations.get(0).getNamespacePermissions().contains(NamespacePermissionEnum.READ), is(true));
        assertThat(namespaceAuthorizations.get(0).getNamespacePermissions().contains(NamespacePermissionEnum.WRITE), is(true));
        assertThat(namespaceAuthorizations.get(0).getNamespacePermissions().contains(NamespacePermissionEnum.WRITE_ATTRIBUTE), is(true));
        assertThat(namespaceAuthorizations.get(0).getNamespacePermissions().contains(NamespacePermissionEnum.WRITE_DESCRIPTIVE_CONTENT), is(true));

        verify(configurationHelper).getBooleanProperty(ConfigurationValue.USER_NAMESPACE_AUTHORIZATION_ENABLED);
        verify(namespaceDao).getNamespaces();
        verifyNoMoreInteractions(userNamespaceAuthorizationDao, wildcardHelper);
    }

    @Test
    public void testBuildNamespaceAuthorizationsWithNamespaceAuthorizationAdmin()
    {
        ApplicationUser applicationUser = new ApplicationUser(getClass());
        applicationUser.setUserId(USER_ID);

        UserEntity userEntity = new UserEntity();
        userEntity.setUserId(USER_ID);
        userEntity.setNamespaceAuthorizationAdmin(true);

        List<NamespaceKey> namespaceKeys = new ArrayList<>();
        namespaceKeys.add(new NamespaceKey(NAMESPACE_CODE));

        when(configurationHelper.getBooleanProperty(ConfigurationValue.USER_NAMESPACE_AUTHORIZATION_ENABLED)).thenReturn(true);
        when(userDao.getUserByUserId(USER_ID)).thenReturn(userEntity);
        when(namespaceDao.getNamespaces()).thenReturn(namespaceKeys);

        userNamespaceAuthorizationHelper.buildNamespaceAuthorizations(applicationUser);

        List<NamespaceAuthorization> namespaceAuthorizations = new ArrayList<>(applicationUser.getNamespaceAuthorizations());

        assertThat(namespaceAuthorizations.get(0).getNamespacePermissions().contains(NamespacePermissionEnum.EXECUTE), is(true));
        assertThat(namespaceAuthorizations.get(0).getNamespacePermissions().contains(NamespacePermissionEnum.GRANT), is(true));
        assertThat(namespaceAuthorizations.get(0).getNamespacePermissions().contains(NamespacePermissionEnum.READ), is(true));
        assertThat(namespaceAuthorizations.get(0).getNamespacePermissions().contains(NamespacePermissionEnum.WRITE), is(true));
        assertThat(namespaceAuthorizations.get(0).getNamespacePermissions().contains(NamespacePermissionEnum.WRITE_ATTRIBUTE), is(true));
        assertThat(namespaceAuthorizations.get(0).getNamespacePermissions().contains(NamespacePermissionEnum.WRITE_DESCRIPTIVE_CONTENT), is(true));

        verify(configurationHelper).getBooleanProperty(ConfigurationValue.USER_NAMESPACE_AUTHORIZATION_ENABLED);
        verify(userDao).getUserByUserId(USER_ID);
        verify(namespaceDao).getNamespaces();
        verifyNoMoreInteractions(userNamespaceAuthorizationDao, wildcardHelper);
    }

    @Test
    public void testGetNamespacePermissions()
    {
        UserNamespaceAuthorizationEntity userNamespaceAuthorizationEntity = new UserNamespaceAuthorizationEntity();
        userNamespaceAuthorizationEntity.setExecutePermission(true);
        userNamespaceAuthorizationEntity.setGrantPermission(true);
        userNamespaceAuthorizationEntity.setReadPermission(true);
        userNamespaceAuthorizationEntity.setWritePermission(true);
        userNamespaceAuthorizationEntity.setWriteAttributePermission(true);
        userNamespaceAuthorizationEntity.setWriteDescriptiveContentPermission(true);

        List<NamespacePermissionEnum> namespacePermissions = userNamespaceAuthorizationHelper.getNamespacePermissions(userNamespaceAuthorizationEntity);

        assertThat(namespacePermissions.contains(NamespacePermissionEnum.EXECUTE), is(true));
        assertThat(namespacePermissions.contains(NamespacePermissionEnum.GRANT), is(true));
        assertThat(namespacePermissions.contains(NamespacePermissionEnum.READ), is(true));
        assertThat(namespacePermissions.contains(NamespacePermissionEnum.WRITE), is(true));
        assertThat(namespacePermissions.contains(NamespacePermissionEnum.WRITE_ATTRIBUTE), is(true));
        assertThat(namespacePermissions.contains(NamespacePermissionEnum.WRITE_DESCRIPTIVE_CONTENT), is(true));
    }

    @Test
    public void testGetAllNamespaceAuthorizations()
    {
        List<NamespaceKey> namespaceKeys = new ArrayList<>();
        namespaceKeys.add(new NamespaceKey(NAMESPACE_CODE));

        when(namespaceDao.getNamespaces()).thenReturn(namespaceKeys);

        List<NamespaceAuthorization> namespaceAuthorizations = new ArrayList<>(userNamespaceAuthorizationHelper.getAllNamespaceAuthorizations());

        assertThat(namespaceAuthorizations.get(0).getNamespace(), is(NAMESPACE_CODE));
        assertThat(namespaceAuthorizations.get(0).getNamespacePermissions().contains(NamespacePermissionEnum.EXECUTE), is(true));
        assertThat(namespaceAuthorizations.get(0).getNamespacePermissions().contains(NamespacePermissionEnum.GRANT), is(true));
        assertThat(namespaceAuthorizations.get(0).getNamespacePermissions().contains(NamespacePermissionEnum.READ), is(true));
        assertThat(namespaceAuthorizations.get(0).getNamespacePermissions().contains(NamespacePermissionEnum.WRITE), is(true));
        assertThat(namespaceAuthorizations.get(0).getNamespacePermissions().contains(NamespacePermissionEnum.WRITE_ATTRIBUTE), is(true));
        assertThat(namespaceAuthorizations.get(0).getNamespacePermissions().contains(NamespacePermissionEnum.WRITE_DESCRIPTIVE_CONTENT), is(true));

        verify(namespaceDao).getNamespaces();
        verifyNoMoreInteractions(namespaceDao, userNamespaceAuthorizationDao, wildcardHelper);
    }
}
