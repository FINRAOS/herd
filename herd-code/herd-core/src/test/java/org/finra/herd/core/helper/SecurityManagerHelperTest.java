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
package org.finra.herd.core.helper;

import java.security.AccessControlException;
import java.security.AllPermission;
import java.security.Permission;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.Collection;
import java.util.PropertyPermission;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test for {@link org.finra.herd.core.helper.SecurityManagerHelper}.
 * The tests in this class do not have meaningful assertions unless the security manager is enabled on the JVM with {@link AllPermission} for the execution of
 * the tests.
 * Each test, nonetheless, should execute without any errors (exceptions or asserts) whether the security manager is enabled or not.
 * The tests do assume however that the security manager is running under {@link AllPermission} enabled. Adding restrictions may cause the tests to break.
 */
public class SecurityManagerHelperTest
{
    /**
     * When {@link org.finra.herd.core.helper.SecurityManagerHelper#doPrivileged(PrivilegedAction)} is called which executes an action restricted by security manager, the method should
     * throw an {@link AccessControlException}.
     * If SecurityManager is disabled, this test asserts that no exception was thrown.
     */
    @Test
    public void testDoPrivilegedWhenNoPermissionsRestrictedActionThrows()
    {
        Class<? extends Exception> expectedExceptionType = null;
        if (SecurityManagerHelper.isSecurityManagerEnabled())
        {
            expectedExceptionType = AccessControlException.class;
        }

        testDoPrivileged(expectedExceptionType, new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
            {
                System.getProperty("test");
                return null;
            }
        });
    }

    /**
     * Asserts that no exceptions are thrown when an action which is not restricted by {@link SecurityManager} is executed under
     * {@link SecurityManagerHelper#doPrivileged(PrivilegedAction)}.
     * This test behaves similarly whether {@link SecurityManager} is enabled or not.
     */
    @Test
    public void testDoPrivilegedWhenNoPermissionsUnrestrictedActionDoesNotThrow()
    {
        testDoPrivileged(null, new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
            {
                /*
                 * Any non-security manager related operations can do.
                 * The simplest operation that could be done is String.length() (nothing particular just some arbitrary operation)
                 */
                "".length();
                return null;
            }
        });
    }

    /**
     * Asserts that an {@link AccessControlException} is thrown when a SecurityManager restricted action is taken under
     * {@link SecurityManagerHelper#doPrivileged(PrivilegedAction, Collection)} where a permission is given, but it does not match the action that is being
     * executed.
     * This test asserts that no exception is thrown when {@link SecurityManager} is disabled.
     */
    @Test
    public void testDoPrivilegedWhenNoMatchingPermission()
    {
        Class<? extends Exception> expectedException = null;
        if (SecurityManagerHelper.isSecurityManagerEnabled())
        {
            expectedException = AccessControlException.class;
        }

        testDoPrivileged(expectedException, new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
            {
                System.getProperty("test");
                return null;
            }
        }, Arrays.<Permission> asList(new PropertyPermission("anotherValue", "read")));
    }

    /**
     * Asserts that no exception is thrown when a security manager restricted action is executed under
     * {@link SecurityManagerHelper#doPrivileged(PrivilegedAction, Collection)} and the permission given matches the action being executed.
     * This test behaves similarly whether {@link SecurityManager} is enabled or not.
     */
    @Test
    public void testDoPrivilegedWhenPermissionMatches()
    {
        testDoPrivileged(null, new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
            {
                System.getProperty("test");
                return null;
            }
        }, Arrays.<Permission> asList(new PropertyPermission("test", "read")));

        testDoPrivileged(null, new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
            {
                System.getProperty("socksProxyHost");
                return null;
            }
        }, Arrays.<Permission> asList(new PropertyPermission("socksProxyHost", "read")));
    }

    /**
     * Asserts what the thrown exception is when {@link SecurityManagerHelper#doPrivileged(PrivilegedAction)} is called. If expectedExceptionType is null, no
     * exceptions are expected to be thrown.
     *
     * @param expectedExceptionType the expected thrown exception, or null
     * @param privilegedAction the action to execute
     */
    private void testDoPrivileged(Class<? extends Exception> expectedExceptionType, PrivilegedAction<Void> privilegedAction)
    {
        try
        {
            SecurityManagerHelper.doPrivileged(privilegedAction);

            if (expectedExceptionType != null)
            {
                Assert.fail("expected " + expectedExceptionType + ", but no exception was thrown");
            }
        }
        catch (Exception e)
        {
            if (expectedExceptionType == null)
            {
                Assert.fail("unexpected exception thrown " + e);
            }
            else
            {
                Assert.assertEquals("thrown exception type", expectedExceptionType, e.getClass());
            }
        }
    }

    /**
     * Asserts what the thrown exception is when {@link SecurityManagerHelper#doPrivileged(PrivilegedAction, Collection)} is called. If expectedExceptionType
     * is null, no exceptions are expected to be thrown.
     *
     * @param expectedExceptionType the expected thrown exception, or null
     * @param privilegedAction the action to execute
     */
    private void testDoPrivileged(Class<? extends Exception> expectedExceptionType, PrivilegedAction<Void> privilegedAction, Collection<Permission> permissions)
    {
        try
        {
            SecurityManagerHelper.doPrivileged(privilegedAction, permissions);

            if (expectedExceptionType != null)
            {
                Assert.fail("expected " + expectedExceptionType + ", but no exception was thrown");
            }
        }
        catch (Exception e)
        {
            if (expectedExceptionType == null)
            {
                Assert.fail("unexpected exception thrown " + e);
            }
            else
            {
                Assert.assertEquals("thrown exception type", expectedExceptionType, e.getClass());
            }
        }
    }
}
