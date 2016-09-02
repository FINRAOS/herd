package org.finra.herd.service.activiti.task;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.google.common.base.Objects;
import org.activiti.engine.delegate.DelegateExecution;
import org.activiti.engine.repository.ProcessDefinition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.web.authentication.preauth.PreAuthenticatedAuthenticationToken;

import org.finra.herd.dao.JobDefinitionDao;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.dto.ApplicationUser;
import org.finra.herd.model.dto.SecurityUserWrapper;
import org.finra.herd.model.jpa.JobDefinitionEntity;
import org.finra.herd.service.AbstractServiceTest;
import org.finra.herd.service.ActivitiService;
import org.finra.herd.service.activiti.ActivitiHelper;
import org.finra.herd.service.activiti.ActivitiRuntimeHelper;
import org.finra.herd.service.helper.ConfigurationDaoHelper;
import org.finra.herd.service.helper.HerdErrorInformationExceptionHandler;
import org.finra.herd.service.helper.JobDefinitionDaoHelper;
import org.finra.herd.service.helper.UserNamespaceAuthorizationHelper;

/**
 * Test suite which targets BaseJavaDelegate specifically. This test suite uses Mockito injection annotations (ie. does not depend on Spring injection and
 * dependencies are CGLIB proxies created by Mockito).
 * <p/>
 * This test suite updated Spring security context which means it EACH TEST CASE MUST BE CLEANED UP after execution. Without doing so may cause subsequent tests
 * to not execute properly.
 */
public class BaseJavaDelegateTest extends AbstractServiceTest
{
    @Mock
    private ActivitiHelper activitiHelper;

    @Mock
    private ActivitiRuntimeHelper activitiRuntimeHelper;

    @Mock
    private ActivitiService activitiService;

    @Autowired
    @InjectMocks
    private MockJavaDelegate baseJavaDelegate;

    @Mock
    private ConfigurationDaoHelper configurationDaoHelper;

    @Mock
    private DelegateExecution delegateExecution;

    @Mock
    private HerdErrorInformationExceptionHandler errorInformationExceptionHandler;

    @Mock
    private JobDefinitionDao jobDefinitionDao;

    @Mock
    private JobDefinitionDaoHelper jobDefinitionDaoHelper;

    @Mock
    private UserNamespaceAuthorizationHelper userNamespaceAuthorizationHelper;

    @After
    public void after()
    {
        SecurityContextHolder.clearContext();
    }

    @Before
    public void before()
    {
        initMocks(this);
    }

    @Test
    public void testExecute() throws Exception
    {
        // Set up expected values.
        String jobDefinitionNamespace = "jobDefinitionNamespace";
        String jobDefinitionName = "jobDefinitionName";
        String processDefinitionId = "processDefinitionId";
        String processDefinitionKey = String.format("%s.%s", jobDefinitionNamespace, jobDefinitionName);
        String updatedBy = "updatedBy";
        JobDefinitionEntity jobDefinitionEntity = new JobDefinitionEntity();
        jobDefinitionEntity.setUpdatedBy(updatedBy);

        // Mock dependency methods.
        when(delegateExecution.getProcessDefinitionId()).thenReturn(processDefinitionId);
        ProcessDefinition processDefinition = mock(ProcessDefinition.class);
        when(processDefinition.getKey()).thenReturn(processDefinitionKey);
        when(activitiService.getProcessDefinitionById(any())).thenReturn(processDefinition);
        when(jobDefinitionDaoHelper.getJobDefinitionEntity(any(), any())).thenReturn(jobDefinitionEntity);

        // Execute test method.
        baseJavaDelegate.execute(delegateExecution);

        // Verify dependencies were invoked correctly.
        InOrder inOrder = inOrder(configurationDaoHelper, activitiService, jobDefinitionDaoHelper, userNamespaceAuthorizationHelper, activitiRuntimeHelper);
        inOrder.verify(configurationDaoHelper).checkNotAllowedMethod(baseJavaDelegate.getClass().getCanonicalName());
        inOrder.verify(activitiService).getProcessDefinitionById(processDefinitionId);
        inOrder.verify(jobDefinitionDaoHelper).getJobDefinitionEntity(jobDefinitionNamespace, jobDefinitionName);
        inOrder.verify(userNamespaceAuthorizationHelper).buildNamespaceAuthorizations(applicationUserUserIdEq(updatedBy));
        inOrder.verify(activitiRuntimeHelper).setTaskSuccessInWorkflow(delegateExecution);
        inOrder.verifyNoMoreInteractions();
        verifyNoMoreInteractions(configurationDaoHelper, activitiService, jobDefinitionDaoHelper, userNamespaceAuthorizationHelper, activitiRuntimeHelper);

        // Assert that security context is cleared at the end of execute.
        assertNull(SecurityContextHolder.getContext().getAuthentication());
    }

    @Test
    public void testSetSecurityContext() throws Exception
    {
        // Set up expected values.
        String jobDefinitionNamespace = "jobDefinitionNamespace";
        String jobDefinitionName = "jobDefinitionName";
        String processDefinitionId = "processDefinitionId";
        String processDefinitionKey = String.format("%s.%s", jobDefinitionNamespace, jobDefinitionName);
        String updatedBy = "updatedBy";
        JobDefinitionEntity jobDefinitionEntity = new JobDefinitionEntity();
        jobDefinitionEntity.setUpdatedBy(updatedBy);

        // Mock dependency methods.
        when(delegateExecution.getProcessDefinitionId()).thenReturn(processDefinitionId);
        ProcessDefinition processDefinition = mock(ProcessDefinition.class);
        when(processDefinition.getKey()).thenReturn(processDefinitionKey);
        when(activitiService.getProcessDefinitionById(any())).thenReturn(processDefinition);
        when(jobDefinitionDaoHelper.getJobDefinitionEntity(any(), any())).thenReturn(jobDefinitionEntity);

        // Clear the security context.
        SecurityContextHolder.clearContext();

        // Execute test method.
        baseJavaDelegate.setSecurityContext(delegateExecution);

        // Verify dependencies were invoked correctly.
        InOrder inOrder = inOrder(activitiService, jobDefinitionDaoHelper, userNamespaceAuthorizationHelper);
        inOrder.verify(activitiService).getProcessDefinitionById(processDefinitionId);
        inOrder.verify(jobDefinitionDaoHelper).getJobDefinitionEntity(jobDefinitionNamespace, jobDefinitionName);
        inOrder.verify(userNamespaceAuthorizationHelper).buildNamespaceAuthorizations(applicationUserUserIdEq(updatedBy));
        inOrder.verifyNoMoreInteractions();
        verifyNoMoreInteractions(activitiService, jobDefinitionDaoHelper, userNamespaceAuthorizationHelper);

        // Assert correct user is in the security context.
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        assertAuthenticationUserIdEquals(updatedBy, authentication);
    }

    @Test
    public void testSetSecurityContextProcessDefinitionNoExists() throws Exception
    {
        // Set up expected values.
        String processDefinitionId = "processDefinitionId";

        // Mock dependency methods.
        when(delegateExecution.getProcessDefinitionId()).thenReturn(processDefinitionId);
        when(activitiService.getProcessDefinitionById(any())).thenReturn(null);

        // Clear the security context.
        SecurityContextHolder.clearContext();

        // Try to execute the test method when process definition does not exist.
        try
        {
            baseJavaDelegate.setSecurityContext(delegateExecution);
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Failed to find Activiti process definition for processDefinitionId=\"%s\".", processDefinitionId), e.getMessage());
        }

        // Verify dependencies were invoked correctly.
        InOrder inOrder = inOrder(activitiService);
        inOrder.verify(activitiService).getProcessDefinitionById(processDefinitionId);
        inOrder.verifyNoMoreInteractions();
        verifyNoMoreInteractions(activitiService);

        // Assert that security context is not set.
        assertNull(SecurityContextHolder.getContext().getAuthentication());
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
