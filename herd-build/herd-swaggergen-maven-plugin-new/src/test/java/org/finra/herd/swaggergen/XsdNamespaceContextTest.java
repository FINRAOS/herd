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
package org.finra.herd.swaggergen;

import org.junit.Test;
import static org.junit.Assert.*;

public class XsdNamespaceContextTest extends AbstractTest
{
    @Test
    public void test_1()
    {
        XsdNamespaceContext xsdNamespaceContext = new XsdNamespaceContext();
        String namespaceURI = xsdNamespaceContext.getNamespaceURI("xs");
        assertEquals(XsdNamespaceContext.NS_URI_XSD, namespaceURI);
    }

    @Test
    public void test_2()
    {
        XsdNamespaceContext xsdNamespaceContext = new XsdNamespaceContext();
        try
        {
            xsdNamespaceContext.getNamespaceURI("foo");
            fail();
        }
        catch (Exception e)
        {
            assertEquals(UnsupportedOperationException.class, e.getClass());
            assertEquals("Unknown prefix \"foo\"", e.getMessage());
        }
    }

    @Test
    public void test_3()
    {
        XsdNamespaceContext xsdNamespaceContext = new XsdNamespaceContext();
        try
        {
            xsdNamespaceContext.getPrefix(XsdNamespaceContext.NS_URI_XSD);
            fail();
        }
        catch (Exception e)
        {
            assertEquals(UnsupportedOperationException.class, e.getClass());
            assertEquals("Unknown namespaceURI \"" + XsdNamespaceContext.NS_URI_XSD + "\"", e.getMessage());
        }
    }

    @Test
    public void test_4()
    {
        XsdNamespaceContext xsdNamespaceContext = new XsdNamespaceContext();
        try
        {
            xsdNamespaceContext.getPrefixes(XsdNamespaceContext.NS_URI_XSD);
            fail();
        }
        catch (Exception e)
        {
            assertEquals(UnsupportedOperationException.class, e.getClass());
            assertEquals("Unknown namespaceURI \"" + XsdNamespaceContext.NS_URI_XSD + "\"", e.getMessage());
        }
    }
}
