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

import java.util.Iterator;

import javax.xml.namespace.NamespaceContext;

/**
 * A NamespaceContext which knows how to map XSD namespaces.
 * This is a very simplistic approach to XSD schema namespace registration and will not attempt to cover all possible cases.
 */
public class XsdNamespaceContext implements NamespaceContext
{
    /**
     * The common namespace prefix of XSD elements.
     */
    public static final String NS_PREFIX_XSD = "xs";

    /**
     * The namespace URI of XSD schema.
     */
    public static final String NS_URI_XSD = "http://www.w3.org/2001/XMLSchema";

    @Override
    public String getNamespaceURI(String prefix)
    {
        if (NS_PREFIX_XSD.equals(prefix))
        {
            return NS_URI_XSD;
        }
        throw new UnsupportedOperationException("Unknown prefix \"" + prefix + "\"");
    }

    @Override
    public String getPrefix(String namespaceURI)
    {
        throw new UnsupportedOperationException("Unknown namespaceURI \"" + namespaceURI + "\"");
    }

    @Override
    public Iterator<?> getPrefixes(String namespaceURI)
    {
        throw new UnsupportedOperationException("Unknown namespaceURI \"" + namespaceURI + "\"");
    }

}
