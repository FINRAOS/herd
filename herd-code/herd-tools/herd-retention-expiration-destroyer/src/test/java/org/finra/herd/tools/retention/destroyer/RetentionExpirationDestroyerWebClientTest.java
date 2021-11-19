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
package org.finra.herd.tools.retention.destroyer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.util.Collections;

import com.sun.jersey.api.client.ClientHandlerException;
import org.finra.herd.dao.impl.MockHttpClientOperationsImpl;
import org.finra.herd.sdk.model.BusinessObjectData;
import org.finra.herd.sdk.model.BusinessObjectDataKey;
import org.junit.Test;


public class RetentionExpirationDestroyerWebClientTest extends AbstractRetentionExpirationDestroyerTest
{
    @Test
    public void testDestroyBusinessObjectData() throws Exception
    {
        retentionExpirationDestroyerWebClient.getRegServerAccessParamsDto().setUseSsl(false);
        BusinessObjectDataKey businessObjectDataKey =
                buildBusinessObjectDataKey("test", "test", "test", "test", 0, "test", Collections.singletonList("test"), 0);
        BusinessObjectData result = retentionExpirationDestroyerWebClient.destroyBusinessObjectData(businessObjectDataKey);
        assertNotNull(result);
    }

    @Test
    public void testDestroyBusinessObjectDataException() throws Exception
    {
        retentionExpirationDestroyerWebClient.getRegServerAccessParamsDto().setRegServerHost(MockHttpClientOperationsImpl.HOSTNAME_THROW_IO_EXCEPTION);

        try
        {
            BusinessObjectDataKey businessObjectDataKey =
                    buildBusinessObjectDataKey("test", "test", "test", "test", 0, "test", Collections.singletonList("test"), 0);
            retentionExpirationDestroyerWebClient.destroyBusinessObjectData(businessObjectDataKey);
            fail();
        }
        catch (ClientHandlerException e)
        {
            assertEquals("testThrowIoException", e.getMessage());
        }
    }

    @Test
    public void testDestroyBusinessObjectDataUseSsl() throws Exception
    {
        retentionExpirationDestroyerWebClient.getRegServerAccessParamsDto().setUseSsl(true);
        BusinessObjectDataKey businessObjectDataKey =
                buildBusinessObjectDataKey("test", "test", "test", "test", 0, "test", Collections.singletonList("test"), 0);
        BusinessObjectData result = retentionExpirationDestroyerWebClient.destroyBusinessObjectData(businessObjectDataKey);
        assertNotNull(result);
    }
}
