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

import java.io.StringWriter;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Stack;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.annotation.XmlType;
import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.logging.Log;

/**
 * Generates example XML.
 */
public class ExampleXmlGenerator
{
    // The log to use for logging purposes.
    @SuppressWarnings("PMD.ProperLogger") // Logger is passed into this method from Mojo base class.
    private Log log;

    // An example date/time.
    private static final GregorianCalendar EXAMPLE_GREGORIAN_CALENDAR = new GregorianCalendar(2015, 11, 25, 0, 0, 0);

    // The call stack to determine if we're in an infinite loop or not.
    private Stack<String> callStack = new Stack<>();

    private String exampleXml;

    /**
     * Instantiates an example XML generator.
     *
     * @param log the log.
     * @param clazz the class to generate the example XML on.
     *
     * @throws MojoExecutionException if any problems were encountered.
     */
    public ExampleXmlGenerator(Log log, Class<?> clazz) throws MojoExecutionException
    {
        this.log = log;

        generateExampleXml(clazz);
    }

    /**
     * Gets an example XML body for the specified class.
     *
     * @param clazz the class.
     *
     * @throws MojoExecutionException if any problems were encountered.
     */
    private void generateExampleXml(Class<?> clazz) throws MojoExecutionException
    {
        Object finalInstance = processClass(clazz);
        try
        {
            if (finalInstance == null)
            {
                exampleXml = "";
                log.info("Can't produce XML for a null element for class \"" + clazz + "\" so using the empty string.");
            }
            else
            {
                // Convert the instance to XML and remove the extra \r characters which causes problems when viewing in the Swagger UI
                exampleXml = objectToXml(finalInstance).replaceAll("\r", "");
            }
        }
        catch (JAXBException e)
        {
            throw new MojoExecutionException("Unable to serialize XML for class \"" + clazz.getName() + "\". Reason: " + e.getMessage(), e);
        }
    }

    /**
     * Processes a class by returning the "example" instance of the class.
     *
     * @param clazz the class.
     *
     * @return the instance of the class.
     * @throws MojoExecutionException if any errors were encountered.
     */
    private Object processClass(Class<?> clazz) throws MojoExecutionException
    {
        log.debug("Generating example XML for class \"" + clazz.getName() + "\".");
        Object instance = null;
        try
        {
            if (String.class.isAssignableFrom(clazz))
            {
                instance = "string";
            }
            else if (Integer.class.isAssignableFrom(clazz) || int.class.isAssignableFrom(clazz))
            {
                instance = 0;
            }
            else if (Long.class.isAssignableFrom(clazz) || long.class.isAssignableFrom(clazz))
            {
                instance = 0L;
            }
            else if (BigDecimal.class.isAssignableFrom(clazz))
            {
                instance = BigDecimal.ZERO;
            }
            else if (XMLGregorianCalendar.class.isAssignableFrom(clazz))
            {
                DatatypeFactory datatypeFactory = DatatypeFactory.newInstance();
                instance = datatypeFactory.newXMLGregorianCalendar(EXAMPLE_GREGORIAN_CALENDAR);
            }
            else if (Boolean.class.isAssignableFrom(clazz) || boolean.class.isAssignableFrom(clazz))
            {
                instance = true;
            }
            else if (clazz.isEnum())
            {
                // If we have an enum, use the first value if at least one value is present.
                Enum<?>[] enums = (Enum<?>[]) clazz.getEnumConstants();
                if (enums != null && enums.length > 0)
                {
                    instance = enums[0];
                }
            }
            else if (clazz.getAnnotation(XmlType.class) != null)
            {
                // We can't instantiate interfaces.
                if (!clazz.isInterface())
                {
                    // Create a new instance of the class.
                    instance = clazz.newInstance();

                    // Process the fields of the class, but only if we haven't processed it before (via the call stack). This is to protect from an infinite
                    // loop.
                    if (!callStack.contains(clazz.getName()))
                    {
                        // Push this class on the stack to show we've processed this class.
                        callStack.push(clazz.getName());

                        // Process all the fields of the class.
                        for (Field field : clazz.getDeclaredFields())
                        {
                            field.setAccessible(true);
                            processField(instance, field);
                        }

                        // Remove this class from the stack since we've finished processing it.
                        callStack.pop();
                    }
                }
            }
            else
            {
                // Default to string in all other cases.
                instance = "string";
            }
        }
        catch (IllegalAccessException | InstantiationException | DatatypeConfigurationException e)
        {
            throw new MojoExecutionException("Unable to create example XML for class \"" + clazz.getName() + "\". Reason: " + e.getMessage(), e);
        }
        return instance;
    }

    /**
     * Processes a field of the example model.
     *
     * @param instance the instance to set.
     * @param field the field to set the instance on.
     *
     * @throws MojoExecutionException if any problems were encountered.
     */
    private void processField(Object instance, Field field) throws MojoExecutionException
    {
        try
        {
            log.debug("Processing field \"" + field.getName() + "\".");
            if (!Modifier.isStatic(field.getModifiers()))
            {
                Class<?> fieldClass = field.getType();
                if (Collection.class.isAssignableFrom(fieldClass))
                {
                    Class<?> actualTypeClass = FieldUtils.getCollectionType(field);
                    if (List.class.isAssignableFrom(fieldClass))
                    {
                        List<Object> list = new ArrayList<>();
                        field.set(instance, list);
                        list.add(processClass(actualTypeClass));
                    }
                    else if (Set.class.isAssignableFrom(fieldClass))
                    {
                        Set<Object> set = new HashSet<>();
                        field.set(instance, set);
                        set.add(processClass(actualTypeClass));
                    }
                }
                else
                {
                    field.set(instance, processClass(field.getType()));
                }
            }
        }
        catch (IllegalAccessException e)
        {
            throw new MojoExecutionException("Unable to process field \"" + field.getName() + "\". Reason: " + e.getMessage(), e);
        }
    }

    /**
     * Returns XML representation of the object.
     *
     * @param object the Java object to be serialized.
     *
     * @return the XML representation of this object
     * @throws javax.xml.bind.JAXBException if a JAXB error occurred.
     */
    private String objectToXml(Object object) throws JAXBException
    {
        JAXBContext requestContext = JAXBContext.newInstance(object.getClass());
        Marshaller requestMarshaller = requestContext.createMarshaller();

        requestMarshaller.setProperty(Marshaller.JAXB_ENCODING, StandardCharsets.UTF_8.name());
        requestMarshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);

        StringWriter sw = new StringWriter();
        requestMarshaller.marshal(object, sw);

        return sw.toString();
    }

    /**
     * Gets the produces example XML.
     *
     * @return the example XML.
     */
    public String getExampleXml()
    {
        return exampleXml;
    }
}
