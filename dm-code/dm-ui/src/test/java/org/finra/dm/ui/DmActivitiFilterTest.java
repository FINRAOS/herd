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
package org.finra.dm.ui;

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockFilterConfig;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

/**
 * This class tests various functionality within the DM Activiti Filter.
 */
public class DmActivitiFilterTest extends AbstractUiTest
{
    private static Logger logger = Logger.getLogger(DmActivitiFilterTest.class);

    @Test
    public void testActivitiForwarded() throws Exception
    {
        DmActivitiFilter filterUnderTest = new DmActivitiFilter();
        filterUnderTest.init(new MockFilterConfig());
        MockFilterChain mockChain = new MockFilterChain();
        MockHttpServletRequest req = new MockHttpServletRequest(null, "/api/test.jsp");
        MockHttpServletResponse rsp = new MockHttpServletResponse();

        filterUnderTest.doFilter(req, rsp, mockChain);
        logger.info("Forwarded URL: " + rsp.getForwardedUrl());

        Assert.assertEquals("/activiti/api/test.jsp", rsp.getForwardedUrl());

        mockChain = new MockFilterChain();
        req = new MockHttpServletRequest(null, "/api");
        rsp = new MockHttpServletResponse();

        filterUnderTest.doFilter(req, rsp, mockChain);
        logger.info("Forwarded URL: " + rsp.getForwardedUrl());

        Assert.assertEquals("/activiti/api", rsp.getForwardedUrl());
        filterUnderTest.destroy();
    }

    @Test
    public void testActivitiNotForwarded() throws Exception
    {
        DmActivitiFilter filterUnderTest = new DmActivitiFilter();
        filterUnderTest.init(new MockFilterConfig());
        MockFilterChain mockChain = new MockFilterChain();
        MockHttpServletRequest req = new MockHttpServletRequest(null, "/test/test.jsp");
        MockHttpServletResponse rsp = new MockHttpServletResponse();

        filterUnderTest.doFilter(req, rsp, mockChain);
        logger.info("Forwarded URL: " + rsp.getForwardedUrl());

        Assert.assertNull(rsp.getForwardedUrl());

        mockChain = new MockFilterChain();
        req = new MockHttpServletRequest(null, "/test");
        rsp = new MockHttpServletResponse();

        filterUnderTest.doFilter(req, rsp, mockChain);
        logger.info("Forwarded URL: " + rsp.getForwardedUrl());

        Assert.assertNull(rsp.getForwardedUrl());
        filterUnderTest.destroy();
    }
}
