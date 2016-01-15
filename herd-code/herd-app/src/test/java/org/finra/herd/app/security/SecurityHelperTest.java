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
package org.finra.herd.app.security;

import org.springframework.security.web.authentication.preauth.PreAuthenticatedAuthenticationToken;

import static org.junit.Assert.assertFalse;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import org.finra.herd.app.AbstractAppTest;

/**
 * This class tests the security helper class.
 */
public class SecurityHelperTest extends AbstractAppTest
{
     @Autowired
     private SecurityHelper securityHelper;

     @Test
     public void testIsGeneratedBy() throws Exception
     {
         assertFalse(securityHelper.isUserGeneratedByClass(null, null));
         
         PreAuthenticatedAuthenticationToken authRequest = new PreAuthenticatedAuthenticationToken(null, null);
         assertFalse(securityHelper.isUserGeneratedByClass(authRequest, null));
     }
}
