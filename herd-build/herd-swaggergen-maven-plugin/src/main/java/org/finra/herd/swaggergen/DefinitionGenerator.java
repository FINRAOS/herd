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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlEnum;
import javax.xml.bind.annotation.XmlType;
import javax.xml.datatype.XMLGregorianCalendar;

import io.swagger.models.ModelImpl;
import io.swagger.models.Swagger;
import io.swagger.models.properties.ArrayProperty;
import io.swagger.models.properties.BooleanProperty;
import io.swagger.models.properties.DateTimeProperty;
import io.swagger.models.properties.DecimalProperty;
import io.swagger.models.properties.IntegerProperty;
import io.swagger.models.properties.Property;
import io.swagger.models.properties.RefProperty;
import io.swagger.models.properties.StringProperty;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.logging.Log;

/**
 * Generates Swagger definitions.
 */
public class DefinitionGenerator
{
    // The log to use for logging purposes.
    @SuppressWarnings("PMD.ProperLogger") // Logger is passed into this method from Mojo base class.
    private Log log;

    // The Swagger metadata.
    private Swagger swagger;

    // The classes that we will create examples for.
    private Set<String> exampleClassNames;

    // The model classes.
    private Set<Class<?>> modelClasses;

    // The XSD parser
    private XsdParser xsdParser;

    /**
     * Instantiates a Swagger definition generator which generates the definitions based on the specified parameters.
     *
     * @param log the log.
     * @param swagger the Swagger metadata.
     * @param exampleClassNames the example class names.
     * @param modelClasses the model classes.
     * @param xsdParser the XSD parser
     *
     * @throws MojoExecutionException if any problems were encountered.
     */
    public DefinitionGenerator(Log log, Swagger swagger, Set<String> exampleClassNames, Set<Class<?>> modelClasses, XsdParser xsdParser)
        throws MojoExecutionException
    {
        this.log = log;
        this.swagger = swagger;
        this.exampleClassNames = exampleClassNames;
        this.modelClasses = modelClasses;
        this.xsdParser = xsdParser;

        generateDefinitions();
    }

    /**
     * Generates definitions for the set of model classes.
     *
     * @throws MojoExecutionException if any errors were encountered.
     */
    private void generateDefinitions() throws MojoExecutionException
    {
        for (Class<?> clazz : modelClasses)
        {
            processDefinitionClass(clazz);
        }
    }

    /**
     * Processes a model class which can be converted into a Swagger definition. A model class must be a JAXB XmlType. This method may be called recursively.
     *
     * @param clazz the class to process.
     *
     * @throws MojoExecutionException if the class isn't an XmlType.
     */
    private void processDefinitionClass(Class<?> clazz) throws MojoExecutionException
    {
        log.debug("Processing model class \"" + clazz.getName() + "\"");
        XmlType xmlType = clazz.getAnnotation(XmlType.class);
        if (xmlType == null)
        {
            log.debug("Model class \"" + clazz.getName() + "\" is not an XmlType so it will be skipped.");
        }
        else
        {
            String name = xmlType.name();
            if (!swagger.getDefinitions().containsKey(name))
            {
                ModelImpl model = new ModelImpl();

                if (exampleClassNames.contains(clazz.getSimpleName()))
                {
                    // Only provide examples for root elements. If we do them for child elements, the JSON examples use the XML examples which is a problem.
                    model.setExample(new ExampleXmlGenerator(log, clazz).getExampleXml());
                }

                swagger.addDefinition(name, model);
                model.name(name);

                if (xsdParser != null)
                {
                    model.setDescription(xsdParser.getAnnotation(name));
                }

                for (Field field : clazz.getDeclaredFields())
                {
                    processField(field, model);
                }
            }
        }
    }

    /**
     * Processes a Field of a model class which can be converted into a Swagger definition property. The property is added into the given model. This method may
     * be called recursively.
     *
     * @param field the field to process.
     * @param model model the model.
     *
     * @throws MojoExecutionException if any problems were encountered.
     */
    private void processField(Field field, ModelImpl model) throws MojoExecutionException
    {
        log.debug("Processing field \"" + field.getName() + "\".");
        if (!Modifier.isStatic(field.getModifiers()))
        {
            Property property;
            Class<?> fieldClass = field.getType();
            if (Collection.class.isAssignableFrom(fieldClass))
            {
                property = new ArrayProperty(getPropertyFromType(FieldUtils.getCollectionType(field)));
            }
            else
            {
                property = getPropertyFromType(fieldClass);
            }

            // Set the required field based on the XmlElement that comes from the XSD.
            XmlElement xmlElement = field.getAnnotation(XmlElement.class);
            if (xmlElement != null)
            {
                property.setRequired(xmlElement.required());
            }

            if (xsdParser != null)
            {
                property.setDescription(xsdParser.getAnnotation(model.getName(), field.getName()));
            }

            // Set the property on model.
            model.property(field.getName(), property);
        }
    }

    /**
     * Gets a property from the given fieldType. This method may be called recursively.
     *
     * @param fieldType the field type class.
     *
     * @return the property.
     * @throws MojoExecutionException if any problems were encountered.
     */
    private Property getPropertyFromType(Class<?> fieldType) throws MojoExecutionException
    {
        Property property;
        if (String.class.isAssignableFrom(fieldType))
        {
            property = new StringProperty();
        }
        else if (Integer.class.isAssignableFrom(fieldType) || int.class.isAssignableFrom(fieldType) || Long.class.isAssignableFrom(fieldType) ||
            long.class.isAssignableFrom(fieldType))
        {
            property = new IntegerProperty();
        }
        else if (BigDecimal.class.isAssignableFrom(fieldType))
        {
            property = new DecimalProperty();
        }
        else if (XMLGregorianCalendar.class.isAssignableFrom(fieldType))
        {
            property = new DateTimeProperty();
        }
        else if (Boolean.class.isAssignableFrom(fieldType) || boolean.class.isAssignableFrom(fieldType))
        {
            property = new BooleanProperty();
        }
        else if (Collection.class.isAssignableFrom(fieldType))
        {
            property = new ArrayProperty(new StringProperty());
        }
        else if (fieldType.getAnnotation(XmlEnum.class) != null)
        {
            /*
             * Enums are a string property which have enum constants
             */
            List<String> enums = new ArrayList<>();
            for (Enum<?> anEnum : (Enum<?>[]) fieldType.getEnumConstants())
            {
                enums.add(anEnum.name());
            }
            property = new StringProperty()._enum(enums);
        }
        /*
         * Recursively process complex objects which is a XmlType
         */
        else if (fieldType.getAnnotation(XmlType.class) != null)
        {
            processDefinitionClass(fieldType);
            property = new RefProperty(fieldType.getAnnotation(XmlType.class).name());
        }
        else
        {
            // Default to a string property in other cases.
            property = new StringProperty();
        }
        log.debug("Field type \"" + fieldType.getName() + "\" is a property type \"" + property.getType() + "\".");
        return property;
    }
}
