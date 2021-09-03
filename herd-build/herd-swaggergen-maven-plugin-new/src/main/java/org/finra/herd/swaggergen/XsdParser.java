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

import java.io.IOException;
import java.io.InputStream;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.springframework.core.io.Resource;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

/**
 * XSD parser and wrapper which makes it easy to retrieve data from an XSD.
 */
public class XsdParser
{
    /**
     * The XPath instance to use.
     */
    private static XPath xPath;

    /**
     * Java String format of the xpath expression for finding complexType elements from the XSD.
     */
    private static final String COMPLEX_TYPE_FORMAT = "/xs:schema/xs:complexType[@name='%s']/xs:annotation/xs:documentation";

    /**
     * Java String format of the xpath expression for finding elements under a complexType from the XSD.
     */
    private static final String COMPLEX_TYPE_ELEMENT_FORMAT = "/xs:schema/xs:complexType[@name='%s']//xs:element[@name='%s']/xs:annotation/xs:documentation";

    static
    {
        xPath = XPathFactory.newInstance().newXPath();
        xPath.setNamespaceContext(new XsdNamespaceContext());
    }

    /**
     * The XSD document to parse.
     */
    private Document document;

    /**
     * Creates a new XSD parser from the given XSD. The XSD with the specified name will be search in the current classpath.
     * 
     * @param xsdName Name of the XSD file.
     */
    public XsdParser(String xsdName)
    {
        try
        {
            Resource[] resources = ResourceUtils.getResources("classpath*:**/" + xsdName);
            if (resources.length < 1)
            {
                throw new IllegalStateException("No XSD found with name \"" + xsdName + "\"");
            }
            setDocument(resources[0].getInputStream());
        }
        catch (IOException e)
        {
            throw new IllegalStateException("Error reading resource \"" + xsdName + "\"", e);
        }
    }

    /**
     * Sets the document to parse. Parses the document from the given input stream.
     * 
     * @param inputStream Input stream to parse
     */
    private void setDocument(InputStream inputStream)
    {
        DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
        documentBuilderFactory.setNamespaceAware(true); // This must be true to make xpath work with namespaces
        try
        {
            document = documentBuilderFactory.newDocumentBuilder().parse(inputStream);
        }
        catch (SAXException | IOException | ParserConfigurationException e)
        {
            throw new IllegalStateException("Error parsing XSD", e);
        }
    }

    /**
     * Gets the documentation of an annotation of the specified complex type.
     * 
     * @param complexTypeName Name of the complexType element
     * @return The annotation
     */
    public String getAnnotation(String complexTypeName)
    {
        return evaluate(String.format(COMPLEX_TYPE_FORMAT, complexTypeName));
    }

    /**
     * Gets the documentation of an annotation of the specified element under the specified complex type.
     * 
     * @param complexTypeName The name of the complex type element
     * @param elementName The name of the element under the complex type
     * @return The annotation
     */
    public String getAnnotation(String complexTypeName, String elementName)
    {
        return evaluate(String.format(COMPLEX_TYPE_ELEMENT_FORMAT, complexTypeName, elementName));
    }

    /**
     * Evaluates the given xpath expression as a string.
     * 
     * @param expression The xpath expression
     * @return The string result of the xpath evaluation.
     */
    private String evaluate(String expression)
    {
        try
        {
            String result = (String) xPath.evaluate(expression, document, XPathConstants.STRING);
            /*
             * XPath will always return an empty string when value is not found.
             */
            if (result.isEmpty())
            {
                result = null;
            }
            return result;
        }
        catch (XPathExpressionException e)
        {
            throw new IllegalStateException(e);
        }
    }
}
