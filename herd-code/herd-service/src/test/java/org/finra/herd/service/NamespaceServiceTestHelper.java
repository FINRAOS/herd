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
package org.finra.herd.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.springframework.stereotype.Component;

import org.finra.herd.model.api.xml.Namespace;
import org.finra.herd.model.api.xml.NamespaceCreateRequest;

@Component
public class NamespaceServiceTestHelper
{
    /**
     * Creates a namespace create request.
     *
     * @param namespaceCode the namespace code
     *
     * @return the newly created namespace create request
     */
    public NamespaceCreateRequest createNamespaceCreateRequest(String namespaceCode)
    {
        NamespaceCreateRequest request = new NamespaceCreateRequest();
        request.setNamespaceCode(namespaceCode);
        return request;
    }

    /**
     * Validates namespace contents against specified parameters.
     *
     * @param expectedNamespaceCode the expected namespace code
     * @param actualNamespace the namespace object instance to be validated
     */
    public void validateNamespace(String expectedNamespaceCode, Namespace actualNamespace)
    {
        assertNotNull(actualNamespace);
        assertEquals(expectedNamespaceCode, actualNamespace.getNamespaceCode());
    }
}
