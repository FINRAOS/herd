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
package org.finra.herd.tools.common.databridge;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;

import java.util.GregorianCalendar;

import javax.xml.datatype.DatatypeFactory;

import org.junit.Test;

import org.finra.herd.core.AbstractCoreTest;
import org.finra.herd.model.api.xml.AwsCredential;

public class AutoRefreshCredentialProviderTest extends AbstractCoreTest
{
    /**
     * The getAwsCredential method should return the cached credential if the session hasn't expired. If the session expired, a new credential should be
     * generated.
     *
     * @throws Exception
     */
    @Test
    public void testAssertCredentialCachingBehavior() throws Exception
    {
        AutoRefreshCredentialProvider autoRefreshCredentialProvider = new AutoRefreshCredentialProvider()
        {
            @Override
            public AwsCredential getNewAwsCredential() throws Exception
            {
                AwsCredential awsCredential = new AwsCredential();
                GregorianCalendar cal = new GregorianCalendar();
                cal.setTimeInMillis(System.currentTimeMillis() + 1000);
                awsCredential.setAwsSessionExpirationTime(DatatypeFactory.newInstance().newXMLGregorianCalendar(cal));
                return awsCredential;
            }
        };

        AwsCredential firstAwsCredential = autoRefreshCredentialProvider.getAwsCredential();
        AwsCredential secondAwsCredential = autoRefreshCredentialProvider.getAwsCredential();
        assertEquals(firstAwsCredential, secondAwsCredential);
        Thread.sleep(1000);
        secondAwsCredential = autoRefreshCredentialProvider.getAwsCredential();
        assertNotEquals(firstAwsCredential, secondAwsCredential);
    }

    @Test
    public void testAssertException()
    {
        AutoRefreshCredentialProvider autoRefreshCredentialProvider = new AutoRefreshCredentialProvider()
        {
            @Override
            public AwsCredential getNewAwsCredential() throws Exception
            {
                throw new Exception("test");
            }
        };

        try
        {
            executeWithoutLogging(AutoRefreshCredentialProvider.class, () -> {
                autoRefreshCredentialProvider.getAwsCredential();
            });
            fail();
        }
        catch (Exception e)
        {
            assertEquals(IllegalStateException.class, e.getClass());
            Throwable cause = e.getCause();
            assertEquals(Exception.class, cause.getClass());
            assertEquals("test", cause.getMessage());
        }
    }
}
