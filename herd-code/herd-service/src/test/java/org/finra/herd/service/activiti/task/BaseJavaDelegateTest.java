package org.finra.herd.service.activiti.task;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.google.common.base.Objects;
import org.activiti.engine.delegate.DelegateExecution;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.web.authentication.preauth.PreAuthenticatedAuthenticationToken;

import org.finra.herd.dao.JobDefinitionDao;
import org.finra.herd.model.dto.ApplicationUser;
import org.finra.herd.model.dto.SecurityUserWrapper;
import org.finra.herd.model.jpa.JobDefinitionEntity;
import org.finra.herd.service.activiti.ActivitiHelper;
import org.finra.herd.service.activiti.ActivitiRuntimeHelper;
import org.finra.herd.service.helper.ConfigurationDaoHelper;
import org.finra.herd.service.helper.HerdErrorInformationExceptionHandler;
import org.finra.herd.service.helper.UserNamespaceAuthorizationHelper;

/**
 * Test suite which targets BaseJavaDelegate specifically. This test suite uses Mockito injection annotations (ie. does not depend on Spring injection and
 * dependencies are CGLIB proxies created by Mockito).
 * <p/>
 * This test suite updated Spring security context which means it EACH TEST CASE MUST BE CLEANED UP after execution. Without doing so may cause subsequent tests
 * to not execute properly.
 */
public class BaseJavaDelegateTest
{
    @InjectMocks
    private BaseJavaDelegate baseJavaDelegate;

    @Mock
    private DelegateExecution delegateExecution;

    @Mock
    private ConfigurationDaoHelper configurationDaoHelper;

    @Mock
    private ActivitiHelper activitiHelper;

    @Mock
    private ActivitiRuntimeHelper activitiRuntimeHelper;

    @Mock
    private JobDefinitionDao jobDefinitionDao;

    @Mock
    private UserNamespaceAuthorizationHelper userNamespaceAuthorizationHelper;

    @Mock
    private HerdErrorInformationExceptionHandler errorInformationExceptionHandler;

    @Before
    public void before()
    {
        baseJavaDelegate = new BaseJavaDelegate()
        {
            @Override
            public void executeImpl(DelegateExecution execution) throws Exception
            {
            }
        };
        initMocks(this);
    }

    @After
    public void after()
    {
        SecurityContextHolder.clearContext();
    }

    @Test
    public void testCheckPermissionsAssertSecurityContextOverwrittenWhenUserAlreadyInContext() throws Exception
    {
        // Set up expected values
        String expectedProcessDefinitionId = "processDefinitionId";
        String expectedUpdatedBy = "updatedBy";
        JobDefinitionEntity jobDefinitionEntity = new JobDefinitionEntity();
        jobDefinitionEntity.setUpdatedBy(expectedUpdatedBy);

        // Mock dependency methods
        when(delegateExecution.getProcessDefinitionId()).thenReturn(expectedProcessDefinitionId);
        when(jobDefinitionDao.getJobDefinitionByProcessDefinitionId(any())).thenReturn(jobDefinitionEntity);

        SecurityContextHolder.getContext().setAuthentication(new TestingAuthenticationToken("", ""));

        // Execute test method
        baseJavaDelegate.execute(delegateExecution);

        // Verify dependencies were invoked correctly
        InOrder inOrder = inOrder(configurationDaoHelper, activitiRuntimeHelper, jobDefinitionDao, userNamespaceAuthorizationHelper);
        inOrder.verify(configurationDaoHelper).checkNotAllowedMethod(baseJavaDelegate.getClass().getCanonicalName());
        inOrder.verify(jobDefinitionDao).getJobDefinitionByProcessDefinitionId(expectedProcessDefinitionId);
        inOrder.verify(userNamespaceAuthorizationHelper).buildNamespaceAuthorizations(applicationUserUserIdEq(expectedUpdatedBy));
        inOrder.verify(activitiRuntimeHelper).setTaskSuccessInWorkflow(delegateExecution);
        inOrder.verifyNoMoreInteractions();
        verifyNoMoreInteractions(configurationDaoHelper, activitiRuntimeHelper, jobDefinitionDao, userNamespaceAuthorizationHelper);

        // Assert correct user is in the security context
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        assertAuthenticationUserIdEquals(expectedUpdatedBy, authentication);
    }

    @Test
    public void testCheckPermissionsAssertSecurityContextUpdatedWhenJobDefinitionHasUpdatedUser() throws Exception
    {
        // Set up expected values
        String expectedProcessDefinitionId = "processDefinitionId";
        String expectedUpdatedBy = "updatedBy";
        JobDefinitionEntity jobDefinitionEntity = new JobDefinitionEntity();
        jobDefinitionEntity.setUpdatedBy(expectedUpdatedBy);

        // Mock dependency methods
        when(delegateExecution.getProcessDefinitionId()).thenReturn(expectedProcessDefinitionId);
        when(jobDefinitionDao.getJobDefinitionByProcessDefinitionId(any())).thenReturn(jobDefinitionEntity);

        // Execute test method
        baseJavaDelegate.execute(delegateExecution);

        // Verify dependencies were invoked correctly
        InOrder inOrder = inOrder(configurationDaoHelper, activitiRuntimeHelper, jobDefinitionDao, userNamespaceAuthorizationHelper);
        inOrder.verify(configurationDaoHelper).checkNotAllowedMethod(baseJavaDelegate.getClass().getCanonicalName());
        inOrder.verify(jobDefinitionDao).getJobDefinitionByProcessDefinitionId(expectedProcessDefinitionId);
        inOrder.verify(userNamespaceAuthorizationHelper).buildNamespaceAuthorizations(applicationUserUserIdEq(expectedUpdatedBy));
        inOrder.verify(activitiRuntimeHelper).setTaskSuccessInWorkflow(delegateExecution);
        inOrder.verifyNoMoreInteractions();
        verifyNoMoreInteractions(configurationDaoHelper, activitiRuntimeHelper, jobDefinitionDao, userNamespaceAuthorizationHelper);

        // Assert correct user is in the security context
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        assertAuthenticationUserIdEquals(expectedUpdatedBy, authentication);
    }

    @Test
    public void testCheckPermissionsAssertNoAccessDeniedWhenJobDefinitionEntityDoesNotExist() throws Exception
    {
        // Set up expected values
        String expectedProcessDefinitionId = "processDefinitionId";

        // Mock dependency methods
        when(delegateExecution.getProcessDefinitionId()).thenReturn(expectedProcessDefinitionId);
        when(jobDefinitionDao.getJobDefinitionByProcessDefinitionId(any())).thenReturn(null);

        // Execute test method
        baseJavaDelegate.execute(delegateExecution);

        InOrder inOrder = inOrder(configurationDaoHelper, activitiRuntimeHelper, jobDefinitionDao);
        inOrder.verify(configurationDaoHelper).checkNotAllowedMethod(baseJavaDelegate.getClass().getCanonicalName());
        inOrder.verify(jobDefinitionDao).getJobDefinitionByProcessDefinitionId(expectedProcessDefinitionId);
        inOrder.verify(activitiRuntimeHelper).setTaskSuccessInWorkflow(delegateExecution);
        inOrder.verifyNoMoreInteractions();
        verifyNoMoreInteractions(configurationDaoHelper, activitiRuntimeHelper, jobDefinitionDao);
    }

    /**
     * Returns a Mockito matcher which matches when the ApplicationUser's user ID equals the given user ID.
     *
     * @param userId The user ID to match
     *
     * @return Mockito proxy matcher of ApplicationUser
     */
    private ApplicationUser applicationUserUserIdEq(String userId)
    {
        return argThat(new ArgumentMatcher<ApplicationUser>()
        {
            @Override
            public boolean matches(Object argument)
            {
                ApplicationUser applicationUser = (ApplicationUser) argument;
                return Objects.equal(userId, applicationUser.getUserId());
            }
        });
    }

    /**
     * Asserts the given actual authentication's user ID is equal to the given expected user ID
     *
     * @param expectedUserId Expected user ID
     * @param actualAuthentication Actual authentication object
     */
    private void assertAuthenticationUserIdEquals(String expectedUserId, Authentication actualAuthentication)
    {
        assertNotNull(actualAuthentication);
        assertEquals(PreAuthenticatedAuthenticationToken.class, actualAuthentication.getClass());
        PreAuthenticatedAuthenticationToken preAuthenticatedAuthenticationToken = (PreAuthenticatedAuthenticationToken) actualAuthentication;
        Object principal = preAuthenticatedAuthenticationToken.getPrincipal();
        assertNotNull(principal);
        assertEquals(SecurityUserWrapper.class, principal.getClass());
        SecurityUserWrapper securityUserWrapper = (SecurityUserWrapper) principal;
        assertEquals(expectedUserId, securityUserWrapper.getUsername());
        assertNotNull(securityUserWrapper.getApplicationUser());
        assertEquals(expectedUserId, securityUserWrapper.getApplicationUser().getUserId());
    }
}
