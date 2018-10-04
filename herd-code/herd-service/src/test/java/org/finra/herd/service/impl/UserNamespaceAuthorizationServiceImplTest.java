package org.finra.herd.service.impl;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.List;

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.dao.UserNamespaceAuthorizationDao;
import org.finra.herd.model.api.xml.NamespacePermissionEnum;
import org.finra.herd.model.api.xml.UserAuthorizations;
import org.finra.herd.model.api.xml.UserNamespaceAuthorization;
import org.finra.herd.model.api.xml.UserNamespaceAuthorizationCreateRequest;
import org.finra.herd.model.api.xml.UserNamespaceAuthorizationKey;
import org.finra.herd.model.api.xml.UserNamespaceAuthorizationUpdateRequest;
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.model.jpa.UserNamespaceAuthorizationEntity;
import org.finra.herd.service.CurrentUserService;
import org.finra.herd.service.MessageNotificationEventService;
import org.finra.herd.service.UserNamespaceAuthorizationService;
import org.finra.herd.service.helper.AlternateKeyHelper;
import org.finra.herd.service.helper.NamespaceDaoHelper;
import org.finra.herd.service.helper.UserNamespaceAuthorizationHelper;

/**
 * This class tests functionality within the user namespace authorization service implementation.
 */
public class UserNamespaceAuthorizationServiceImplTest
{
    private static final String USER_ID_VALUE = "testUserId";

    private static final String NAMESPACE_VALUE = "testNamespace";

    private static final int USER_NAMESPACE_AUTHORIZATION_ID = 10;

    private static final List<NamespacePermissionEnum> NAMESPACE_PERMISSIONS = ImmutableList.of(NamespacePermissionEnum.READ);

    private static final UserNamespaceAuthorizationKey USER_NAMESPACE_AUTHORIZATION_KEY = new UserNamespaceAuthorizationKey(USER_ID_VALUE, NAMESPACE_VALUE);

    private static final UserNamespaceAuthorizationCreateRequest USER_NAMESPACE_AUTHORIZATION_CREATE_REQUEST =
        new UserNamespaceAuthorizationCreateRequest(USER_NAMESPACE_AUTHORIZATION_KEY, NAMESPACE_PERMISSIONS);

    private static final UserNamespaceAuthorizationUpdateRequest USER_NAMESPACE_AUTHORIZATION_UPDATE_REQUEST =
        new UserNamespaceAuthorizationUpdateRequest(NAMESPACE_PERMISSIONS);

    private static final NamespaceEntity NAMESPACE_ENTITY = new NamespaceEntity()
    {{
        setCode(NAMESPACE_VALUE);
    }};

    private static final UserNamespaceAuthorizationEntity USER_NAMESPACE_AUTHORIZATION_ENTITY = new UserNamespaceAuthorizationEntity()
    {{
        setId(USER_NAMESPACE_AUTHORIZATION_ID);
        setUserId(USER_ID_VALUE);
        setNamespace(NAMESPACE_ENTITY);
        setReadPermission(true);
    }};

    private static final UserAuthorizations USER_AUTHORIZATIONS = new UserAuthorizations()
    {{
        setUserId("currentUser");
    }};

    @InjectMocks
    private UserNamespaceAuthorizationService userNamespaceAuthorizationService = new UserNamespaceAuthorizationServiceImpl();

    @Mock
    private AlternateKeyHelper alternateKeyHelper;

    @Mock
    private CurrentUserService currentUserService;

    @Mock
    private NamespaceDaoHelper namespaceDaoHelper;

    @Mock
    private UserNamespaceAuthorizationDao userNamespaceAuthorizationDao;

    @Mock
    private UserNamespaceAuthorizationHelper userNamespaceAuthorizationHelper;

    @Mock
    private MessageNotificationEventService messageNotificationEventService;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCreateUserNamespaceAuthorizationHappyPath()
    {
        when(alternateKeyHelper.validateStringParameter("namespace", NAMESPACE_VALUE)).thenReturn(NAMESPACE_VALUE);
        when(alternateKeyHelper.validateStringParameter("user id", USER_ID_VALUE)).thenReturn(USER_ID_VALUE);
        when(userNamespaceAuthorizationDao.getUserNamespaceAuthorizationByKey(USER_NAMESPACE_AUTHORIZATION_KEY)).thenReturn(null);
        when(namespaceDaoHelper.getNamespaceEntity(NAMESPACE_VALUE)).thenReturn(NAMESPACE_ENTITY);
        when(userNamespaceAuthorizationDao.saveAndRefresh(any())).thenReturn(USER_NAMESPACE_AUTHORIZATION_ENTITY);
        when(userNamespaceAuthorizationHelper.getNamespacePermissions(USER_NAMESPACE_AUTHORIZATION_ENTITY)).thenReturn(NAMESPACE_PERMISSIONS);

        UserNamespaceAuthorization userNamespaceAuthorization =
            userNamespaceAuthorizationService.createUserNamespaceAuthorization(USER_NAMESPACE_AUTHORIZATION_CREATE_REQUEST);

        assertEquals(USER_NAMESPACE_AUTHORIZATION_ID, userNamespaceAuthorization.getId());
        assertEquals(USER_NAMESPACE_AUTHORIZATION_KEY, userNamespaceAuthorization.getUserNamespaceAuthorizationKey());
        assertEquals(NAMESPACE_PERMISSIONS, userNamespaceAuthorization.getNamespacePermissions());

        verify(alternateKeyHelper).validateStringParameter("namespace", NAMESPACE_VALUE);
        verify(alternateKeyHelper).validateStringParameter("user id", USER_ID_VALUE);
        verify(namespaceDaoHelper).getNamespaceEntity(NAMESPACE_VALUE);
        verify(userNamespaceAuthorizationDao).getUserNamespaceAuthorizationByKey(USER_NAMESPACE_AUTHORIZATION_KEY);
        verify(userNamespaceAuthorizationDao).saveAndRefresh(any(UserNamespaceAuthorizationEntity.class));
        verify(messageNotificationEventService).processUserNamespaceAuthorizationChangeNotificationEvent(USER_NAMESPACE_AUTHORIZATION_KEY);
        verify(userNamespaceAuthorizationHelper).getNamespacePermissions(USER_NAMESPACE_AUTHORIZATION_ENTITY);

        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testUpdateUserNamespaceAuthorizationHappyPath()
    {
        when(alternateKeyHelper.validateStringParameter("namespace", NAMESPACE_VALUE)).thenReturn(NAMESPACE_VALUE);
        when(alternateKeyHelper.validateStringParameter("user id", USER_ID_VALUE)).thenReturn(USER_ID_VALUE);
        when(currentUserService.getCurrentUser()).thenReturn(USER_AUTHORIZATIONS);
        when(userNamespaceAuthorizationDao.getUserNamespaceAuthorizationByKey(USER_NAMESPACE_AUTHORIZATION_KEY))
            .thenReturn(USER_NAMESPACE_AUTHORIZATION_ENTITY);
        when(userNamespaceAuthorizationHelper.getNamespacePermissions(USER_NAMESPACE_AUTHORIZATION_ENTITY)).thenReturn(NAMESPACE_PERMISSIONS);

        UserNamespaceAuthorization userNamespaceAuthorization =
            userNamespaceAuthorizationService.updateUserNamespaceAuthorization(USER_NAMESPACE_AUTHORIZATION_KEY, USER_NAMESPACE_AUTHORIZATION_UPDATE_REQUEST);

        assertEquals(USER_NAMESPACE_AUTHORIZATION_ID, userNamespaceAuthorization.getId());
        assertEquals(USER_NAMESPACE_AUTHORIZATION_KEY, userNamespaceAuthorization.getUserNamespaceAuthorizationKey());
        assertEquals(NAMESPACE_PERMISSIONS, userNamespaceAuthorization.getNamespacePermissions());

        verify(alternateKeyHelper).validateStringParameter("namespace", NAMESPACE_VALUE);
        verify(alternateKeyHelper).validateStringParameter("user id", USER_ID_VALUE);
        verify(currentUserService).getCurrentUser();
        verify(userNamespaceAuthorizationDao).getUserNamespaceAuthorizationByKey(USER_NAMESPACE_AUTHORIZATION_KEY);
        verify(userNamespaceAuthorizationDao).saveAndRefresh(any(UserNamespaceAuthorizationEntity.class));
        verify(messageNotificationEventService).processUserNamespaceAuthorizationChangeNotificationEvent(USER_NAMESPACE_AUTHORIZATION_KEY);
        verify(userNamespaceAuthorizationHelper).getNamespacePermissions(USER_NAMESPACE_AUTHORIZATION_ENTITY);

        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testDeleteUserNamespaceAuthorizationHappyPath()
    {
        when(alternateKeyHelper.validateStringParameter("namespace", NAMESPACE_VALUE)).thenReturn(NAMESPACE_VALUE);
        when(alternateKeyHelper.validateStringParameter("user id", USER_ID_VALUE)).thenReturn(USER_ID_VALUE);
        when(userNamespaceAuthorizationDao.getUserNamespaceAuthorizationByKey(USER_NAMESPACE_AUTHORIZATION_KEY))
            .thenReturn(USER_NAMESPACE_AUTHORIZATION_ENTITY);
        when(userNamespaceAuthorizationHelper.getNamespacePermissions(USER_NAMESPACE_AUTHORIZATION_ENTITY)).thenReturn(NAMESPACE_PERMISSIONS);

        UserNamespaceAuthorization userNamespaceAuthorization =
            userNamespaceAuthorizationService.deleteUserNamespaceAuthorization(USER_NAMESPACE_AUTHORIZATION_KEY);

        assertEquals(USER_NAMESPACE_AUTHORIZATION_ID, userNamespaceAuthorization.getId());
        assertEquals(USER_NAMESPACE_AUTHORIZATION_KEY, userNamespaceAuthorization.getUserNamespaceAuthorizationKey());
        assertEquals(NAMESPACE_PERMISSIONS, userNamespaceAuthorization.getNamespacePermissions());

        verify(alternateKeyHelper).validateStringParameter("namespace", NAMESPACE_VALUE);
        verify(alternateKeyHelper).validateStringParameter("user id", USER_ID_VALUE);
        verify(userNamespaceAuthorizationDao).getUserNamespaceAuthorizationByKey(USER_NAMESPACE_AUTHORIZATION_KEY);
        verify(userNamespaceAuthorizationDao).delete(any(UserNamespaceAuthorizationEntity.class));
        verify(messageNotificationEventService).processUserNamespaceAuthorizationChangeNotificationEvent(USER_NAMESPACE_AUTHORIZATION_KEY);
        verify(userNamespaceAuthorizationHelper).getNamespacePermissions(USER_NAMESPACE_AUTHORIZATION_ENTITY);

        verifyNoMoreInteractionsHelper();
    }

    /**
     * Checks if any of the mocks has any interaction.
     */
    private void verifyNoMoreInteractionsHelper()
    {
        verifyNoMoreInteractions(alternateKeyHelper, currentUserService, namespaceDaoHelper, userNamespaceAuthorizationDao, userNamespaceAuthorizationHelper,
            messageNotificationEventService);
    }
}
