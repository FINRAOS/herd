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

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;

import org.finra.herd.service.AbstractServiceTest;

public class AbstractServiceAdviceTest extends AbstractServiceTest
{
    @InjectMocks
    private AbstractServiceAdvice abstractServiceAdvice = new AbstractServiceAdvice()
    {
        @Override
        protected void serviceMethods()
        {
            return;
        }
    };

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testServiceMethods()
    {
        // Call the method under test.
        abstractServiceAdvice.serviceMethods();
    }
}
