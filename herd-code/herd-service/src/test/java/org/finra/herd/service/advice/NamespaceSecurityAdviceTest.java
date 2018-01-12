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
package org.finra.herd.service.advice;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.reflect.MethodSignature;
import org.junit.After;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;

import org.finra.herd.model.annotation.NamespacePermission;
import org.finra.herd.model.annotation.NamespacePermissions;
import org.finra.herd.model.api.xml.BusinessObjectDataNotificationFilter;
import org.finra.herd.model.api.xml.BusinessObjectDataNotificationRegistrationCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionCreateRequest;
import org.finra.herd.model.api.xml.JobAction;
import org.finra.herd.model.api.xml.NamespaceAuthorization;
import org.finra.herd.model.api.xml.NamespacePermissionEnum;
import org.finra.herd.model.api.xml.NotificationRegistrationKey;
import org.finra.herd.model.dto.ApplicationUser;
import org.finra.herd.model.dto.SecurityUserWrapper;
import org.finra.herd.service.AbstractServiceTest;
import org.finra.herd.service.BusinessObjectDefinitionService;

public class NamespaceSecurityAdviceTest extends AbstractServiceTest
{
    @Autowired
    private NamespaceSecurityAdvice namespaceSecurityAdvice;

    @Autowired
    @Qualifier(value = "businessObjectDefinitionServiceImpl")
    private BusinessObjectDefinitionService businessObjectDefinitionServiceImpl;

    @After
    public void after()
    {
        SecurityContextHolder.clearContext();
    }

    /**
     * Asserts that the namespace security advice is enabled. Try calling a secured method with a mock user in the context with invalid permissions. The
     * expectation is that the method call fails with AccessDeniedException if the advice is enabled.
     */
    @Test
    public void assertAdviceEnabled()
    {
        // put a fake user with no permissions into the security context
        // the security context is cleared on the after() method of this test suite
        String username = "username";
        Class<?> generatedByClass = getClass();
        ApplicationUser applicationUser = new ApplicationUser(generatedByClass);
        applicationUser.setUserId(username);
        applicationUser.setNamespaceAuthorizations(Collections.emptySet());
        SecurityContextHolder.getContext().setAuthentication(
            new TestingAuthenticationToken(new SecurityUserWrapper(username, "password", false, false, false, false, Collections.emptyList(), applicationUser),
                null));

        try
        {
            businessObjectDefinitionServiceImpl
                .createBusinessObjectDefinition(new BusinessObjectDefinitionCreateRequest(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, null, null, null));
            fail();
        }
        catch (Exception e)
        {
            assertEquals(AccessDeniedException.class, e.getClass());
        }
    }

    /**
     * Test case where the current user has both the namespace and the appropriate permissions.
     */
    @Test
    public void checkPermissionAssertNoExceptionWhenHasPermissions() throws Exception
    {
        // Mock a join point of the method call
        // mockMethod("foo");
        JoinPoint joinPoint = mock(JoinPoint.class);
        MethodSignature methodSignature = mock(MethodSignature.class);
        Method method = NamespaceSecurityAdviceTest.class.getDeclaredMethod("mockMethod", String.class);
        when(methodSignature.getParameterNames()).thenReturn(new String[] {"namespace"});
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(joinPoint.getArgs()).thenReturn(new Object[] {"foo"});

        String userId = "userId";
        ApplicationUser applicationUser = new ApplicationUser(getClass());
        applicationUser.setUserId(userId);
        applicationUser.setNamespaceAuthorizations(new HashSet<>());
        applicationUser.getNamespaceAuthorizations().add(new NamespaceAuthorization("foo", Arrays.asList(NamespacePermissionEnum.READ)));
        SecurityContextHolder.getContext().setAuthentication(
            new TestingAuthenticationToken(new SecurityUserWrapper(userId, "", false, false, false, false, Arrays.asList(), applicationUser), null));

        try
        {
            namespaceSecurityAdvice.checkPermission(joinPoint);
        }
        catch (AccessDeniedException e)
        {
            fail();
        }
    }

    /**
     * Test the case where user has the namespace but does not have the permission
     */
    @Test
    public void checkPermissionAssertAccessDeniedWhenCurrentUserHasWrongPermissionType() throws Exception
    {
        // Mock a join point of the method call
        // mockMethod("foo");
        JoinPoint joinPoint = mock(JoinPoint.class);
        MethodSignature methodSignature = mock(MethodSignature.class);
        Method method = NamespaceSecurityAdviceTest.class.getDeclaredMethod("mockMethod", String.class);
        when(methodSignature.getParameterNames()).thenReturn(new String[] {"namespace"});
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(joinPoint.getArgs()).thenReturn(new Object[] {"foo"});

        String userId = "userId";
        ApplicationUser applicationUser = new ApplicationUser(getClass());
        applicationUser.setUserId(userId);
        applicationUser.setNamespaceAuthorizations(new HashSet<>());
        // User has WRITE permissions, but the method requires READ
        applicationUser.getNamespaceAuthorizations().add(new NamespaceAuthorization("foo", Arrays.asList(NamespacePermissionEnum.WRITE)));
        SecurityContextHolder.getContext().setAuthentication(
            new TestingAuthenticationToken(new SecurityUserWrapper(userId, "", false, false, false, false, Arrays.asList(), applicationUser), null));

        try
        {
            namespaceSecurityAdvice.checkPermission(joinPoint);
            fail();
        }
        catch (Exception e)
        {
            assertEquals(AccessDeniedException.class, e.getClass());
            assertEquals(String.format("User \"%s\" does not have \"[READ]\" permission(s) to the namespace \"foo\"", userId), e.getMessage());
        }
    }

    @Test
    public void checkPermissionAssertNoExceptionWhenNoSecurityContext() throws Exception
    {
        // Mock a join point of the method call
        // mockMethod("foo");
        JoinPoint joinPoint = mock(JoinPoint.class);
        MethodSignature methodSignature = mock(MethodSignature.class);
        Method method = NamespaceSecurityAdviceTest.class.getDeclaredMethod("mockMethod", String.class);
        when(methodSignature.getParameterNames()).thenReturn(new String[] {"namespace"});
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(joinPoint.getArgs()).thenReturn(new Object[] {"foo"});

        try
        {
            namespaceSecurityAdvice.checkPermission(joinPoint);
        }
        catch (AccessDeniedException e)
        {
            fail();
        }
    }

    /**
     * Test the case where user has the appropriate permission, but not for the namespace the method is invoked.
     */
    @Test
    public void checkPermissionAssertAccessDeniedWhenCurrentUserHasWrongNamespace() throws Exception
    {
        // Mock a join point of the method call
        // mockMethod("foo");
        JoinPoint joinPoint = mock(JoinPoint.class);
        MethodSignature methodSignature = mock(MethodSignature.class);
        Method method = NamespaceSecurityAdviceTest.class.getDeclaredMethod("mockMethod", String.class);
        when(methodSignature.getParameterNames()).thenReturn(new String[] {"namespace"});
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(joinPoint.getArgs()).thenReturn(new Object[] {"foo"});

        String userId = "userId";
        ApplicationUser applicationUser = new ApplicationUser(getClass());
        applicationUser.setUserId(userId);
        applicationUser.setNamespaceAuthorizations(new HashSet<>());
        // User has READ but for namespace "bar", not "foo"
        applicationUser.getNamespaceAuthorizations().add(new NamespaceAuthorization("bar", Arrays.asList(NamespacePermissionEnum.READ)));
        SecurityContextHolder.getContext().setAuthentication(
            new TestingAuthenticationToken(new SecurityUserWrapper(userId, "", false, false, false, false, Arrays.asList(), applicationUser), null));

        try
        {
            namespaceSecurityAdvice.checkPermission(joinPoint);
            fail();
        }
        catch (Exception e)
        {
            assertEquals(AccessDeniedException.class, e.getClass());
            assertEquals(String.format("User \"%s\" does not have \"[READ]\" permission(s) to the namespace \"foo\"", userId), e.getMessage());
        }
    }

    /**
     * Test where a method is annotated with multiple NamespacePermission annotations. Asserts that the user will all permissions do not throw an exception.
     */
    @Test
    public void checkPermissionAssertNoExceptionWhenMultipleAnnotationsAndAllPermissionsValid() throws Exception
    {
        // Mock a join point of the method call
        // mockMethodMultipleAnnotations("foo", "bar");
        JoinPoint joinPoint = mock(JoinPoint.class);
        MethodSignature methodSignature = mock(MethodSignature.class);
        Method method = NamespaceSecurityAdviceTest.class.getDeclaredMethod("mockMethodMultipleAnnotations", String.class, String.class);
        when(methodSignature.getParameterNames()).thenReturn(new String[] {"namespace1", "namespace2"});
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(joinPoint.getArgs()).thenReturn(new Object[] {"foo", "bar"});

        String userId = "userId";
        ApplicationUser applicationUser = new ApplicationUser(getClass());
        applicationUser.setUserId(userId);
        applicationUser.setNamespaceAuthorizations(new HashSet<>());
        applicationUser.getNamespaceAuthorizations().add(new NamespaceAuthorization("foo", Arrays.asList(NamespacePermissionEnum.READ)));
        applicationUser.getNamespaceAuthorizations().add(new NamespaceAuthorization("bar", Arrays.asList(NamespacePermissionEnum.WRITE)));
        SecurityContextHolder.getContext().setAuthentication(
            new TestingAuthenticationToken(new SecurityUserWrapper(userId, "", false, false, false, false, Arrays.asList(), applicationUser), null));

        try
        {
            namespaceSecurityAdvice.checkPermission(joinPoint);
        }
        catch (AccessDeniedException e)
        {
            fail();
        }
    }

    /**
     * Test where a method with multiple annotation is called, but the user does not have permission to one of the namespaces. Asserts that the check throws
     * AccessDenied.
     */
    @Test
    public void checkPermissionAssertAccessDeniedWhenMultipleAnnotationsAndUserHasOneWrongPermission() throws Exception
    {
        // Mock a join point of the method call
        // mockMethodMultipleAnnotations("foo", "bar");
        JoinPoint joinPoint = mock(JoinPoint.class);
        MethodSignature methodSignature = mock(MethodSignature.class);
        Method method = NamespaceSecurityAdviceTest.class.getDeclaredMethod("mockMethodMultipleAnnotations", String.class, String.class);
        when(methodSignature.getParameterNames()).thenReturn(new String[] {"namespace1", "namespace2"});
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(joinPoint.getArgs()).thenReturn(new Object[] {"foo", "bar"});

        String userId = "userId";
        ApplicationUser applicationUser = new ApplicationUser(getClass());
        applicationUser.setUserId(userId);
        applicationUser.setNamespaceAuthorizations(new HashSet<>());
        applicationUser.getNamespaceAuthorizations().add(new NamespaceAuthorization("foo", Arrays.asList(NamespacePermissionEnum.READ)));
        SecurityContextHolder.getContext().setAuthentication(
            new TestingAuthenticationToken(new SecurityUserWrapper(userId, "", false, false, false, false, Arrays.asList(), applicationUser), null));

        try
        {
            namespaceSecurityAdvice.checkPermission(joinPoint);
            fail();
        }
        catch (Exception e)
        {
            assertEquals(AccessDeniedException.class, e.getClass());
            assertEquals(String.format("User \"%s\" does not have \"[WRITE]\" permission(s) to the namespace \"bar\"", userId), e.getMessage());
        }
    }

    @Test
    public void checkPermissionAssertNoExceptionWhenComplexCaseAndUserHasAllPermissions() throws Exception
    {
        // Mock a join point of the method call
        // mockMethod(request);
        JoinPoint joinPoint = mock(JoinPoint.class);
        MethodSignature methodSignature = mock(MethodSignature.class);
        Method method = NamespaceSecurityAdviceTest.class.getDeclaredMethod("mockMethod", BusinessObjectDataNotificationRegistrationCreateRequest.class);
        when(methodSignature.getParameterNames()).thenReturn(new String[] {"request"});
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        BusinessObjectDataNotificationRegistrationCreateRequest request = new BusinessObjectDataNotificationRegistrationCreateRequest();
        request.setBusinessObjectDataNotificationRegistrationKey(new NotificationRegistrationKey("ns1", null));
        request.setBusinessObjectDataNotificationFilter(new BusinessObjectDataNotificationFilter("ns2", null, null, null, null, null, null, null));
        request.setJobActions(Arrays.asList(new JobAction("ns3", null, null), new JobAction("ns4", null, null)));
        when(joinPoint.getArgs()).thenReturn(new Object[] {request});

        String userId = "userId";
        ApplicationUser applicationUser = new ApplicationUser(getClass());
        applicationUser.setUserId(userId);
        applicationUser.setNamespaceAuthorizations(new HashSet<>());
        applicationUser.getNamespaceAuthorizations().add(new NamespaceAuthorization("ns1", Arrays.asList(NamespacePermissionEnum.WRITE)));
        applicationUser.getNamespaceAuthorizations().add(new NamespaceAuthorization("ns2", Arrays.asList(NamespacePermissionEnum.READ)));
        applicationUser.getNamespaceAuthorizations().add(new NamespaceAuthorization("ns3", Arrays.asList(NamespacePermissionEnum.EXECUTE)));
        applicationUser.getNamespaceAuthorizations().add(new NamespaceAuthorization("ns4", Arrays.asList(NamespacePermissionEnum.EXECUTE)));
        SecurityContextHolder.getContext().setAuthentication(
            new TestingAuthenticationToken(new SecurityUserWrapper(userId, "", false, false, false, false, Arrays.asList(), applicationUser), null));

        try
        {
            namespaceSecurityAdvice.checkPermission(joinPoint);
        }
        catch (AccessDeniedException e)
        {
            fail();
        }
    }

    @Test
    public void checkPermissionAssertAccessDeniedWhenComplexCaseAndUserHasWrongPermission() throws Exception
    {
        // Mock a join point of the method call
        // mockMethod(request);
        JoinPoint joinPoint = mock(JoinPoint.class);
        MethodSignature methodSignature = mock(MethodSignature.class);
        Method method = NamespaceSecurityAdviceTest.class.getDeclaredMethod("mockMethod", BusinessObjectDataNotificationRegistrationCreateRequest.class);
        when(methodSignature.getParameterNames()).thenReturn(new String[] {"request"});
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        BusinessObjectDataNotificationRegistrationCreateRequest request = new BusinessObjectDataNotificationRegistrationCreateRequest();
        request.setBusinessObjectDataNotificationRegistrationKey(new NotificationRegistrationKey("ns1", null));
        request.setBusinessObjectDataNotificationFilter(new BusinessObjectDataNotificationFilter("ns2", null, null, null, null, null, null, null));
        request.setJobActions(Arrays.asList(new JobAction("ns3", null, null), new JobAction("ns4", null, null)));
        when(joinPoint.getArgs()).thenReturn(new Object[] {request});

        String userId = "userId";
        ApplicationUser applicationUser = new ApplicationUser(getClass());
        applicationUser.setUserId(userId);
        applicationUser.setNamespaceAuthorizations(new HashSet<>());
        applicationUser.getNamespaceAuthorizations().add(new NamespaceAuthorization("ns1", Arrays.asList(NamespacePermissionEnum.WRITE)));
        applicationUser.getNamespaceAuthorizations().add(new NamespaceAuthorization("ns2", Arrays.asList(NamespacePermissionEnum.READ)));
        applicationUser.getNamespaceAuthorizations().add(new NamespaceAuthorization("ns3", Arrays.asList(NamespacePermissionEnum.EXECUTE)));
        // User does not have the expected EXECUTE permission on ns4
        applicationUser.getNamespaceAuthorizations().add(new NamespaceAuthorization("ns4", Arrays.asList(NamespacePermissionEnum.READ)));
        SecurityContextHolder.getContext().setAuthentication(
            new TestingAuthenticationToken(new SecurityUserWrapper(userId, "", false, false, false, false, Arrays.asList(), applicationUser), null));

        try
        {
            namespaceSecurityAdvice.checkPermission(joinPoint);
            fail();
        }
        catch (Exception e)
        {
            assertEquals(AccessDeniedException.class, e.getClass());
            assertEquals(String.format("User \"%s\" does not have \"[EXECUTE]\" permission(s) to the namespace \"ns4\"", userId), e.getMessage());
        }
    }

    @Test
    public void checkPermissionAssertErrorWhenAnnotationFieldRefersToNonString() throws Exception
    {
        // Mock a join point of the method call
        // mockMethod(1);
        JoinPoint joinPoint = mock(JoinPoint.class);
        MethodSignature methodSignature = mock(MethodSignature.class);
        Method method = NamespaceSecurityAdviceTest.class.getDeclaredMethod("mockMethod", Integer.class);
        when(methodSignature.getParameterNames()).thenReturn(new String[] {"aNumber"});
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(joinPoint.getArgs()).thenReturn(new Object[] {1});

        String userId = "userId";
        ApplicationUser applicationUser = new ApplicationUser(getClass());
        applicationUser.setUserId(userId);
        applicationUser.setNamespaceAuthorizations(new HashSet<>());
        SecurityContextHolder.getContext().setAuthentication(
            new TestingAuthenticationToken(new SecurityUserWrapper(userId, "", false, false, false, false, Arrays.asList(), applicationUser), null));

        try
        {
            namespaceSecurityAdvice.checkPermission(joinPoint);
            fail();
        }
        catch (Exception e)
        {
            assertEquals(IllegalStateException.class, e.getClass());
            assertEquals("Object must be of type class java.lang.String or interface java.util.Collection. Actual object.class = class java.lang.Integer",
                e.getMessage());
        }
    }

    @Test
    public void checkPermissionAssertNoErrorWhenMethodDoesNotHaveAnnotations() throws Exception
    {
        // Mock a join point of the method call
        // mockMethod(1);
        JoinPoint joinPoint = mock(JoinPoint.class);
        MethodSignature methodSignature = mock(MethodSignature.class);
        Method method = NamespaceSecurityAdviceTest.class.getDeclaredMethod("mockMethod");
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(joinPoint.getArgs()).thenReturn(new Object[] {});

        String userId = "userId";
        ApplicationUser applicationUser = new ApplicationUser(getClass());
        applicationUser.setUserId(userId);
        applicationUser.setNamespaceAuthorizations(new HashSet<>());
        SecurityContextHolder.getContext().setAuthentication(
            new TestingAuthenticationToken(new SecurityUserWrapper(userId, "", false, false, false, false, Arrays.asList(), applicationUser), null));

        try
        {
            namespaceSecurityAdvice.checkPermission(joinPoint);
        }
        catch (AccessDeniedException e)
        {
            fail();
        }
    }

    @Test
    public void checkPermissionAssertNoErrorWhenUserHasMultiplePermissions() throws Exception
    {
        // Mock a join point of the method call
        // mockMethod("foo");
        JoinPoint joinPoint = mock(JoinPoint.class);
        MethodSignature methodSignature = mock(MethodSignature.class);
        Method method = NamespaceSecurityAdviceTest.class.getDeclaredMethod("mockMethod", String.class);
        when(methodSignature.getMethod()).thenReturn(method);
        when(methodSignature.getParameterNames()).thenReturn(new String[] {"namespace"});
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(joinPoint.getArgs()).thenReturn(new Object[] {"foo"});

        String userId = "userId";
        ApplicationUser applicationUser = new ApplicationUser(getClass());
        applicationUser.setUserId(userId);
        applicationUser.setNamespaceAuthorizations(new HashSet<>());
        applicationUser.getNamespaceAuthorizations()
            .add(new NamespaceAuthorization("foo", Arrays.asList(NamespacePermissionEnum.READ, NamespacePermissionEnum.WRITE)));
        SecurityContextHolder.getContext().setAuthentication(
            new TestingAuthenticationToken(new SecurityUserWrapper(userId, "", false, false, false, false, Arrays.asList(), applicationUser), null));

        try
        {
            namespaceSecurityAdvice.checkPermission(joinPoint);
        }
        catch (AccessDeniedException e)
        {
            fail();
        }
    }

    @Test
    public void checkPermissionAssertAccessDeniedWhenCurrentUserHasNoAnyRequiredPermissions() throws Exception
    {
        // Mock a join point of the method call
        // mockMethod("foo");
        JoinPoint joinPoint = mock(JoinPoint.class);
        MethodSignature methodSignature = mock(MethodSignature.class);
        Method method = NamespaceSecurityAdviceTest.class.getDeclaredMethod("mockMethodMultiplePermissions", String.class);
        when(methodSignature.getParameterNames()).thenReturn(new String[] {"namespace"});
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(joinPoint.getArgs()).thenReturn(new Object[] {"foo"});

        String userId = "userId";
        ApplicationUser applicationUser = new ApplicationUser(getClass());
        applicationUser.setUserId(userId);
        applicationUser.setNamespaceAuthorizations(new HashSet<>());
        applicationUser.getNamespaceAuthorizations().add(new NamespaceAuthorization("foo", Arrays.asList(NamespacePermissionEnum.WRITE_DESCRIPTIVE_CONTENT)));
        SecurityContextHolder.getContext().setAuthentication(
            new TestingAuthenticationToken(new SecurityUserWrapper(userId, "", false, false, false, false, Arrays.asList(), applicationUser), null));

        try
        {
            namespaceSecurityAdvice.checkPermission(joinPoint);
            fail();
        }
        catch (Exception e)
        {
            assertEquals(AccessDeniedException.class, e.getClass());
            assertEquals(String.format("User \"%s\" does not have \"[READ OR WRITE]\" permission(s) to the namespace \"foo\"", userId), e.getMessage());
        }
    }

    @Test
    public void checkPermissionAssertAccessApprovedWhenUserRequiresMultiplePermissionsButIsMissingOne() throws Exception
    {
        // Mock a join point of the method call
        // mockMethodMultiplePermissions("foo");
        JoinPoint joinPoint = mock(JoinPoint.class);
        MethodSignature methodSignature = mock(MethodSignature.class);
        Method method = NamespaceSecurityAdviceTest.class.getDeclaredMethod("mockMethodMultiplePermissions", String.class);
        when(methodSignature.getMethod()).thenReturn(method);
        when(methodSignature.getParameterNames()).thenReturn(new String[] {"namespace"});
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(joinPoint.getArgs()).thenReturn(new Object[] {"foo"});

        String userId = "userId";
        ApplicationUser applicationUser = new ApplicationUser(getClass());
        applicationUser.setUserId(userId);
        applicationUser.setNamespaceAuthorizations(new HashSet<>());
        // User requires both READ and WRITE, but only has READ
        // It works now as the permissions in the same space are treated using OR logic now
        applicationUser.getNamespaceAuthorizations().add(new NamespaceAuthorization("foo", Arrays.asList(NamespacePermissionEnum.READ)));
        SecurityContextHolder.getContext().setAuthentication(
            new TestingAuthenticationToken(new SecurityUserWrapper(userId, "", false, false, false, false, Arrays.asList(), applicationUser), null));

        try
        {
            namespaceSecurityAdvice.checkPermission(joinPoint);
        }
        catch (Exception e)
        {
            fail();
        }
    }



    @Test
    public void checkPermissionAssertAccessDeniedWhenCurrentUserHasNullAuthorizations() throws Exception
    {
        // Mock a join point of the method call
        // mockMethod("foo");
        JoinPoint joinPoint = mock(JoinPoint.class);
        MethodSignature methodSignature = mock(MethodSignature.class);
        Method method = NamespaceSecurityAdviceTest.class.getDeclaredMethod("mockMethod", String.class);
        when(methodSignature.getParameterNames()).thenReturn(new String[] {"namespace"});
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(joinPoint.getArgs()).thenReturn(new Object[] {"foo"});

        String userId = "userId";
        ApplicationUser applicationUser = new ApplicationUser(getClass());
        applicationUser.setUserId(userId);
        applicationUser.setNamespaceAuthorizations(null);
        SecurityContextHolder.getContext().setAuthentication(
            new TestingAuthenticationToken(new SecurityUserWrapper(userId, "", false, false, false, false, Arrays.asList(), applicationUser), null));

        try
        {
            namespaceSecurityAdvice.checkPermission(joinPoint);
            fail();
        }
        catch (Exception e)
        {
            assertEquals(AccessDeniedException.class, e.getClass());
            assertEquals(String.format("User \"%s\" does not have \"[READ]\" permission(s) to the namespace \"foo\"", userId), e.getMessage());
        }
    }

    @Test
    public void checkPermissionAssertAccessDeniedWhenCurrentUserHasNullPermissions() throws Exception
    {
        // Mock a join point of the method call
        // mockMethod("foo");
        JoinPoint joinPoint = mock(JoinPoint.class);
        MethodSignature methodSignature = mock(MethodSignature.class);
        Method method = NamespaceSecurityAdviceTest.class.getDeclaredMethod("mockMethod", String.class);
        when(methodSignature.getParameterNames()).thenReturn(new String[] {"namespace"});
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(joinPoint.getArgs()).thenReturn(new Object[] {"foo"});

        String userId = "userId";
        ApplicationUser applicationUser = new ApplicationUser(getClass());
        applicationUser.setUserId(userId);
        applicationUser.setNamespaceAuthorizations(new HashSet<>());
        applicationUser.getNamespaceAuthorizations().add(new NamespaceAuthorization("foo", null));
        SecurityContextHolder.getContext().setAuthentication(
            new TestingAuthenticationToken(new SecurityUserWrapper(userId, "", false, false, false, false, Arrays.asList(), applicationUser), null));

        try
        {
            namespaceSecurityAdvice.checkPermission(joinPoint);
            fail();
        }
        catch (Exception e)
        {
            assertEquals(AccessDeniedException.class, e.getClass());
            assertEquals(String.format("User \"%s\" does not have \"[READ]\" permission(s) to the namespace \"foo\"", userId), e.getMessage());
        }
    }

    @Test
    public void checkPermissionAssertAccessDeniedWhenApplicationUserIsNull() throws Exception
    {
        // Mock a join point of the method call
        // mockMethod("foo");
        JoinPoint joinPoint = mock(JoinPoint.class);
        MethodSignature methodSignature = mock(MethodSignature.class);
        Method method = NamespaceSecurityAdviceTest.class.getDeclaredMethod("mockMethod", String.class);
        when(methodSignature.getParameterNames()).thenReturn(new String[] {"namespace"});
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(joinPoint.getArgs()).thenReturn(new Object[] {"foo"});

        String userId = "userId";
        SecurityContextHolder.getContext()
            .setAuthentication(new TestingAuthenticationToken(new SecurityUserWrapper(userId, "", false, false, false, false, Arrays.asList(), null), null));

        try
        {
            namespaceSecurityAdvice.checkPermission(joinPoint);
            fail();
        }
        catch (Exception e)
        {
            assertEquals(AccessDeniedException.class, e.getClass());
            assertEquals("Current user does not have \"[READ]\" permission(s) to the namespace \"foo\"", e.getMessage());
        }
    }

    @Test
    public void checkPermissionAssertAccessDeniedWhenPrincipalIsNotSecurityUserWrapper() throws Exception
    {
        // Mock a join point of the method call
        // mockMethod("foo");
        JoinPoint joinPoint = mock(JoinPoint.class);
        MethodSignature methodSignature = mock(MethodSignature.class);
        Method method = NamespaceSecurityAdviceTest.class.getDeclaredMethod("mockMethod", String.class);
        when(methodSignature.getParameterNames()).thenReturn(new String[] {"namespace"});
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(joinPoint.getArgs()).thenReturn(new Object[] {"foo"});

        SecurityContextHolder.getContext().setAuthentication(new TestingAuthenticationToken("streetcreds", null));

        try
        {
            namespaceSecurityAdvice.checkPermission(joinPoint);
            fail();
        }
        catch (Exception e)
        {
            assertEquals(AccessDeniedException.class, e.getClass());
            assertEquals("Current user does not have \"[READ]\" permission(s) to the namespace \"foo\"", e.getMessage());
        }
    }

    @Test
    public void checkPermissionAssertAccessDeniedWhenPrincipalIsNull() throws Exception
    {
        // Mock a join point of the method call
        // mockMethod("foo");
        JoinPoint joinPoint = mock(JoinPoint.class);
        MethodSignature methodSignature = mock(MethodSignature.class);
        Method method = NamespaceSecurityAdviceTest.class.getDeclaredMethod("mockMethod", String.class);
        when(methodSignature.getParameterNames()).thenReturn(new String[] {"namespace"});
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(joinPoint.getArgs()).thenReturn(new Object[] {"foo"});

        SecurityContextHolder.getContext().setAuthentication(new TestingAuthenticationToken(null, null));

        try
        {
            namespaceSecurityAdvice.checkPermission(joinPoint);
            fail();
        }
        catch (Exception e)
        {
            assertEquals(AccessDeniedException.class, e.getClass());
            assertEquals("Current user does not have \"[READ]\" permission(s) to the namespace \"foo\"", e.getMessage());
        }
    }

    @Test
    public void checkPermissionAssertNoExceptionWhenHasPermissionsNamespaceIgnoreCase() throws Exception
    {
        // Mock a join point of the method call
        // mockMethod("foo");
        JoinPoint joinPoint = mock(JoinPoint.class);
        MethodSignature methodSignature = mock(MethodSignature.class);
        Method method = NamespaceSecurityAdviceTest.class.getDeclaredMethod("mockMethod", String.class);
        when(methodSignature.getParameterNames()).thenReturn(new String[] {"namespace"});
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(joinPoint.getArgs()).thenReturn(new Object[] {"foo"});

        String userId = "userId";
        ApplicationUser applicationUser = new ApplicationUser(getClass());
        applicationUser.setUserId(userId);
        applicationUser.setNamespaceAuthorizations(new HashSet<>());
        // user has permission to capital "FOO" and needs permission to lowercase "foo"
        applicationUser.getNamespaceAuthorizations().add(new NamespaceAuthorization("FOO", Arrays.asList(NamespacePermissionEnum.READ)));
        SecurityContextHolder.getContext().setAuthentication(
            new TestingAuthenticationToken(new SecurityUserWrapper(userId, "", false, false, false, false, Arrays.asList(), applicationUser), null));

        try
        {
            namespaceSecurityAdvice.checkPermission(joinPoint);
        }
        catch (AccessDeniedException e)
        {
            fail();
        }
    }

    @Test
    public void checkPermissionAssertNoExceptionWhenNamespaceBlank() throws Exception
    {
        // Mock a join point of the method call
        // mockMethod(" ");
        JoinPoint joinPoint = mock(JoinPoint.class);
        MethodSignature methodSignature = mock(MethodSignature.class);
        Method method = NamespaceSecurityAdviceTest.class.getDeclaredMethod("mockMethod", String.class);
        when(methodSignature.getParameterNames()).thenReturn(new String[] {"namespace"});
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(joinPoint.getArgs()).thenReturn(new Object[] {BLANK_TEXT});

        String userId = "userId";
        ApplicationUser applicationUser = new ApplicationUser(getClass());
        applicationUser.setUserId(userId);
        applicationUser.setNamespaceAuthorizations(new HashSet<>());
        SecurityContextHolder.getContext().setAuthentication(
            new TestingAuthenticationToken(new SecurityUserWrapper(userId, "", false, false, false, false, Arrays.asList(), applicationUser), null));

        try
        {
            namespaceSecurityAdvice.checkPermission(joinPoint);
        }
        catch (AccessDeniedException e)
        {
            fail();
        }
    }

    @Test
    public void checkPermissionAssertNoExceptionWhenHasPermissionsNamespaceTrimmed() throws Exception
    {
        // Mock a join point of the method call
        // mockMethod(" foo ");
        JoinPoint joinPoint = mock(JoinPoint.class);
        MethodSignature methodSignature = mock(MethodSignature.class);
        Method method = NamespaceSecurityAdviceTest.class.getDeclaredMethod("mockMethod", String.class);
        when(methodSignature.getParameterNames()).thenReturn(new String[] {"namespace"});
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(joinPoint.getArgs()).thenReturn(new Object[] {BLANK_TEXT + "foo" + BLANK_TEXT});

        String userId = "userId";
        ApplicationUser applicationUser = new ApplicationUser(getClass());
        applicationUser.setUserId(userId);
        applicationUser.setNamespaceAuthorizations(new HashSet<>());
        // User has permission to "foo" but the actual namespace given is " foo "
        applicationUser.getNamespaceAuthorizations().add(new NamespaceAuthorization("foo", Arrays.asList(NamespacePermissionEnum.READ)));
        SecurityContextHolder.getContext().setAuthentication(
            new TestingAuthenticationToken(new SecurityUserWrapper(userId, "", false, false, false, false, Arrays.asList(), applicationUser), null));

        try
        {
            namespaceSecurityAdvice.checkPermission(joinPoint);
        }
        catch (AccessDeniedException e)
        {
            fail();
        }
    }

    @Test
    public void checkPermissionAssertAccessDeniedWhenNoPermissionsNamespaceTrimmed() throws Exception
    {
        // Mock a join point of the method call
        // mockMethod(" foo ");
        JoinPoint joinPoint = mock(JoinPoint.class);
        MethodSignature methodSignature = mock(MethodSignature.class);
        Method method = NamespaceSecurityAdviceTest.class.getDeclaredMethod("mockMethod", String.class);
        when(methodSignature.getParameterNames()).thenReturn(new String[] {"namespace"});
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(joinPoint.getArgs()).thenReturn(new Object[] {BLANK_TEXT + "foo" + BLANK_TEXT});

        String userId = "userId";
        ApplicationUser applicationUser = new ApplicationUser(getClass());
        applicationUser.setUserId(userId);
        applicationUser.setNamespaceAuthorizations(new HashSet<>());
        // User has permission to "bar" but the actual namespace given is " foo "
        applicationUser.getNamespaceAuthorizations().add(new NamespaceAuthorization("bar", Arrays.asList(NamespacePermissionEnum.READ)));
        SecurityContextHolder.getContext().setAuthentication(
            new TestingAuthenticationToken(new SecurityUserWrapper(userId, "", false, false, false, false, Arrays.asList(), applicationUser), null));

        try
        {
            namespaceSecurityAdvice.checkPermission(joinPoint);
            fail();
        }
        catch (Exception e)
        {
            assertEquals(AccessDeniedException.class, e.getClass());
            assertEquals(String.format("User \"%s\" does not have \"[READ]\" permission(s) to the namespace \"foo\"", userId), e.getMessage());
        }
    }

    @Test
    public void checkPermissionAssertMultipleAccessDeniedExceptionsAreGatheredIntoSingleMessageWhenMultipleAnnotations() throws Exception
    {
        // Mock a join point of the method call
        // mockMethodMultipleAnnotations("namespace1", "namespace2");
        JoinPoint joinPoint = mock(JoinPoint.class);
        MethodSignature methodSignature = mock(MethodSignature.class);
        Method method = NamespaceSecurityAdviceTest.class.getDeclaredMethod("mockMethodMultipleAnnotations", String.class, String.class);
        when(methodSignature.getParameterNames()).thenReturn(new String[] {"namespace1", "namespace2"});
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(joinPoint.getArgs()).thenReturn(new Object[] {"foo", "bar"});

        String userId = "userId";
        ApplicationUser applicationUser = new ApplicationUser(getClass());
        applicationUser.setUserId(userId);
        applicationUser.setNamespaceAuthorizations(new HashSet<>());
        // User has no permissions
        SecurityContextHolder.getContext().setAuthentication(
            new TestingAuthenticationToken(new SecurityUserWrapper(userId, "", false, false, false, false, Arrays.asList(), applicationUser), null));

        try
        {
            namespaceSecurityAdvice.checkPermission(joinPoint);
            fail();
        }
        catch (Exception e)
        {
            assertEquals(AccessDeniedException.class, e.getClass());
            assertEquals(String.format("User \"%s\" does not have \"[READ]\" permission(s) to the namespace \"foo\"%n" +
                "User \"%s\" does not have \"[WRITE]\" permission(s) to the namespace \"bar\"", userId, userId), e.getMessage());
        }
    }

    @Test
    public void checkPermissionAssertMultipleAccessDeniedExceptionsAreGatheredIntoSingleMessageWhenCollections() throws Exception
    {
        // Mock a join point of the method call
        // mockMethod({"", ""})
        JoinPoint joinPoint = mock(JoinPoint.class);
        MethodSignature methodSignature = mock(MethodSignature.class);
        Method method = NamespaceSecurityAdviceTest.class.getDeclaredMethod("mockMethod", List.class);
        when(methodSignature.getParameterNames()).thenReturn(new String[] {"namespaces"});
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(joinPoint.getArgs()).thenReturn(new Object[] {Arrays.asList("foo", "bar")});

        String userId = "userId";
        ApplicationUser applicationUser = new ApplicationUser(getClass());
        applicationUser.setUserId(userId);
        applicationUser.setNamespaceAuthorizations(new HashSet<>());
        // User has no permissions
        SecurityContextHolder.getContext().setAuthentication(
            new TestingAuthenticationToken(new SecurityUserWrapper(userId, "", false, false, false, false, Arrays.asList(), applicationUser), null));

        try
        {
            namespaceSecurityAdvice.checkPermission(joinPoint);
            fail();
        }
        catch (Exception e)
        {
            assertEquals(AccessDeniedException.class, e.getClass());
            assertEquals(String.format("User \"%s\" does not have \"[READ]\" permission(s) to the namespace \"foo\"%n" +
                "User \"%s\" does not have \"[READ]\" permission(s) to the namespace \"bar\"", userId, userId), e.getMessage());
        }
    }

    /**
     * Assert no access denied exception when parameter value is null.
     */
    @Test
    public void checkPermissionAssertNoExceptionWhenNull() throws Exception
    {
        // Mock a join point of the method call
        // mockMethod(null);
        JoinPoint joinPoint = mock(JoinPoint.class);
        MethodSignature methodSignature = mock(MethodSignature.class);
        Method method = NamespaceSecurityAdviceTest.class.getDeclaredMethod("mockMethod", String.class);
        when(methodSignature.getParameterNames()).thenReturn(new String[] {"namespace"});
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(joinPoint.getArgs()).thenReturn(new Object[] {null});

        String userId = "userId";
        ApplicationUser applicationUser = new ApplicationUser(getClass());
        applicationUser.setUserId(userId);
        applicationUser.setNamespaceAuthorizations(new HashSet<>());
        SecurityContextHolder.getContext().setAuthentication(
            new TestingAuthenticationToken(new SecurityUserWrapper(userId, "", false, false, false, false, Arrays.asList(), applicationUser), null));

        try
        {
            namespaceSecurityAdvice.checkPermission(joinPoint);
        }
        catch (AccessDeniedException e)
        {
            fail();
        }
    }

    /**
     * Do not invoke this method. This method is a test input for reflection related tests.
     */
    @NamespacePermission(fields = "#namespace", permissions = NamespacePermissionEnum.READ)
    private void mockMethod(String namespace)
    {
    }

    /**
     * Do not invoke this method. This method is a test input for reflection related tests. Case where namespace1 requires READ, but namespace2 requires WRITE
     */
    @NamespacePermissions({@NamespacePermission(fields = "#namespace1", permissions = NamespacePermissionEnum.READ), @NamespacePermission(
        fields = "#namespace2", permissions = NamespacePermissionEnum.WRITE)})
    private void mockMethodMultipleAnnotations(String namespace1, String namespace2)
    {
    }

    /**
     * Do not invoke this method. This method is a test input for reflection related tests. The most complex case we have so far: a mock simulation of
     * notification registration, where there are 3 different permissions on namespaces, and one of them is a collection.
     */
    @NamespacePermissions({@NamespacePermission(fields = "#request.businessObjectDataNotificationRegistrationKey.namespace",
        permissions = NamespacePermissionEnum.WRITE), @NamespacePermission(fields = "#request.businessObjectDataNotificationFilter.namespace",
        permissions = NamespacePermissionEnum.READ), @NamespacePermission(fields = "#request.jobActions.![namespace]",
        permissions = NamespacePermissionEnum.EXECUTE)})
    private void mockMethod(BusinessObjectDataNotificationRegistrationCreateRequest request)
    {
    }

    /**
     * Do not invoke this method. This method is a test input for reflection related tests. A case where the method is annotated, but the field refers to a
     * non-string parameter.
     */
    @NamespacePermission(fields = "#aNumber", permissions = NamespacePermissionEnum.READ)
    private void mockMethod(Integer aNumber)
    {
    }

    /**
     * Do not invoke this method. This method is a test input for reflection related tests. Case where a method is not annotated.
     */
    @SuppressWarnings("unused")
    private void mockMethod()
    {
    }

    /**
     * Do not invoke this method. This method is a test input for reflection related tests. A case which the current user requires multiple permissions on the
     * same namespace to access.
     */
    @NamespacePermission(fields = "#namespace", permissions = {NamespacePermissionEnum.READ, NamespacePermissionEnum.WRITE})
    private void mockMethodMultiplePermissions(String namespace)
    {
    }

    /**
     * Do not invoke this method. This method is a test input for reflection related tests.
     */
    @NamespacePermission(fields = "#namespaces", permissions = {NamespacePermissionEnum.READ})
    private void mockMethod(List<String> namespaces)
    {
    }
}
