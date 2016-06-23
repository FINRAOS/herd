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
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.bind.annotation.XmlType;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.models.Operation;
import io.swagger.models.Path;
import io.swagger.models.RefModel;
import io.swagger.models.Response;
import io.swagger.models.Swagger;
import io.swagger.models.Tag;
import io.swagger.models.parameters.BodyParameter;
import io.swagger.models.parameters.PathParameter;
import io.swagger.models.parameters.QueryParameter;
import io.swagger.models.parameters.SerializableParameter;
import io.swagger.models.properties.RefProperty;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.logging.Log;
import org.jboss.forge.roaster.Roaster;
import org.jboss.forge.roaster._shade.org.eclipse.jdt.core.dom.Javadoc;
import org.jboss.forge.roaster._shade.org.eclipse.jdt.core.dom.TagElement;
import org.jboss.forge.roaster.model.JavaDocTag;
import org.jboss.forge.roaster.model.source.AnnotationSource;
import org.jboss.forge.roaster.model.source.JavaClassSource;
import org.jboss.forge.roaster.model.source.JavaDocSource;
import org.jboss.forge.roaster.model.source.MethodSource;
import org.jboss.forge.roaster.model.source.ParameterSource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.core.type.classreading.CachingMetadataReaderFactory;
import org.springframework.core.type.classreading.MetadataReader;
import org.springframework.util.ClassUtils;
import org.springframework.util.CollectionUtils;
import org.springframework.util.SystemPropertyUtils;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Finds and process REST controllers.
 */
public class RestControllerProcessor
{
    // The log to use for logging purposes.
    @SuppressWarnings("PMD.ProperLogger") // Logger is passed into this method from Mojo base class.
    private Log log;

    // The Swagger metadata.
    private Swagger swagger;

    // The REST Java package.
    private String restJavaPackage;

    // The tag pattern to determine REST controller names.
    private Pattern tagPattern;

    // The map of Java class names to their respective source class information.
    private Map<String, JavaClassSource> sourceMap = new HashMap<>();

    // The classes that we will create examples for.
    private Set<String> exampleClassNames = new HashSet<>();

    // The model error class.
    private Class<?> modelErrorClass;

    // A set of operation Id's to keep track of so we don't create duplicates.
    private Set<String> operationIds = new HashSet<>();

    /**
     * Instantiates a REST controller process which finds and processes REST controllers.
     *
     * @param log the log
     * @param swagger the Swagger metadata
     * @param restJavaPackage the REST Java package.
     * @param tagPatternTemplate the tag pattern template.
     * @param modelErrorClass the model error class.
     *
     * @throws MojoExecutionException if any problems were encountered.
     */
    public RestControllerProcessor(Log log, Swagger swagger, String restJavaPackage, String tagPatternTemplate, Class<?> modelErrorClass)
        throws MojoExecutionException
    {
        this.log = log;
        this.swagger = swagger;
        this.restJavaPackage = restJavaPackage;
        this.modelErrorClass = modelErrorClass;

        // Create the tag pattern based on the parameter.
        tagPattern = Pattern.compile(tagPatternTemplate);

        findAndProcessRestControllers();
    }

    /**
     * Finds all the REST controllers within the configured REST Java package and process the REST methods within each one.
     *
     * @throws MojoExecutionException if any errors were encountered.
     */
    private void findAndProcessRestControllers() throws MojoExecutionException
    {
        try
        {
            log.debug("Finding and processing REST controllers.");

            // Loop through each resources and process each one.
            for (Resource resource : ResourceUtils
                .getResources(ResourcePatternResolver.CLASSPATH_ALL_URL_PREFIX + restJavaPackage.replace('.', '/') + "/**/*.java"))
            {
                if (resource.isReadable())
                {
                    JavaClassSource javaClassSource = Roaster.parse(JavaClassSource.class, resource.getInputStream());
                    sourceMap.put(javaClassSource.getName(), javaClassSource);
                    log.debug("Found Java source class \"" + javaClassSource.getName() + "\".");
                }
            }

            // Loop through each controller resources and process each one.
            for (Resource resource : ResourceUtils.getResources(ResourcePatternResolver.CLASSPATH_ALL_URL_PREFIX +
                ClassUtils.convertClassNameToResourcePath(SystemPropertyUtils.resolvePlaceholders(restJavaPackage)) +
                "/**/*.class"))
            {
                if (resource.isReadable())
                {
                    // Create a resource resolver to fetch resources.
                    MetadataReader metadataReader = new CachingMetadataReaderFactory(new PathMatchingResourcePatternResolver()).getMetadataReader(resource);
                    Class<?> clazz = Class.forName(metadataReader.getClassMetadata().getClassName());
                    processRestControllerClass(clazz);
                }
            }
        }
        catch (ClassNotFoundException | IOException e)
        {
            throw new MojoExecutionException("Error processing REST classes. Reason: " + e.getMessage(), e);
        }
    }

    /**
     * Processes a Spring MVC REST controller class that is annotated with RestController. Also collects any required model objects based on parameters and
     * return types of each endpoint into the specified model classes set.
     *
     * @param clazz the class to process
     *
     * @throws MojoExecutionException if any errors were encountered.
     */
    private void processRestControllerClass(Class<?> clazz) throws MojoExecutionException
    {
        // Get the Java class source information.
        JavaClassSource javaClassSource = sourceMap.get(clazz.getSimpleName());
        if (javaClassSource == null)
        {
            throw new MojoExecutionException("No source resource found for class \"" + clazz.getName() + "\".");
        }

        Api api = clazz.getAnnotation(Api.class);
        boolean hidden = api != null && api.hidden();

        if ((clazz.getAnnotation(RestController.class) != null) && (!hidden))
        {
            log.debug("Processing RestController class \"" + clazz.getName() + "\".");

            // Default the tag name to the simple class name.
            String tagName = clazz.getSimpleName();

            // See if the "Api" annotation exists.
            if (api != null && api.tags().length > 0)
            {
                // The "Api" annotation was found so use it's configured tag.
                tagName = api.tags()[0];
            }
            else
            {
                // No "Api" annotation so try to get the tag name from the class name. If not, we will stick with the default simple class name.
                Matcher matcher = tagPattern.matcher(clazz.getSimpleName());
                if (matcher.find())
                {
                    // If our class has the form
                    tagName = matcher.group("tag");
                }
            }
            log.debug("Using tag name \"" + tagName + "\".");

            // Add the tag and process each method.
            swagger.addTag(new Tag().name(tagName));
            for (Method method : clazz.getDeclaredMethods())
            {
                // Get the method source information.
                List<Class<?>> methodParamClasses = new ArrayList<>();
                for (Parameter parameter : method.getParameters())
                {
                    methodParamClasses.add(parameter.getType());
                }
                MethodSource<JavaClassSource> methodSource =
                    javaClassSource.getMethod(method.getName(), methodParamClasses.toArray(new Class<?>[methodParamClasses.size()]));
                if (methodSource == null)
                {
                    throw new MojoExecutionException(
                        "No method source found for class \"" + clazz.getName() + "\" and method name \"" + method.getName() + "\".");
                }

                // Process the REST controller method along with its source information.
                processRestControllerMethod(method, clazz.getAnnotation(RequestMapping.class), tagName, methodSource);
            }
        }
        else
        {
            log.debug("Skipping class \"" + clazz.getName() + "\" because it is either not a RestController or it is hidden.");
        }
    }

    /**
     * Processes a method in a REST controller which represents an endpoint, that is, it is annotated with RequestMapping.
     *
     * @param method the method.
     * @param classRequestMapping the parent.
     * @param tagName the tag name.
     * @param methodSource the method source information.
     *
     * @throws MojoExecutionException if any errors were encountered.
     */
    @SuppressWarnings("unchecked") // CollectionUtils doesn't work with generics.
    private void processRestControllerMethod(Method method, RequestMapping classRequestMapping, String tagName, MethodSource<JavaClassSource> methodSource)
        throws MojoExecutionException
    {
        log.debug("Processing method \"" + method.getName() + "\".");

        // Build a map of each parameter name to its description from the method Javadoc (i.e. all @param and @return values).
        Map<String, String> methodParamDescriptions = new HashMap<>();
        JavaDocSource<MethodSource<JavaClassSource>> javaDocSource = methodSource.getJavaDoc();
        List<JavaDocTag> tags = javaDocSource.getTags();
        for (JavaDocTag javaDocTag : tags)
        {
            processJavaDocTag(javaDocTag, methodParamDescriptions);
        }

        List<String> produces = Collections.emptyList();
        List<String> consumes = Collections.emptyList();
        List<RequestMethod> requestMethods = Collections.emptyList();
        List<String> uris = Collections.emptyList();

        // If a class request mapping exists, use it as the default.
        if (classRequestMapping != null)
        {
            produces = CollectionUtils.arrayToList(classRequestMapping.produces());
            consumes = CollectionUtils.arrayToList(classRequestMapping.consumes());
            requestMethods = CollectionUtils.arrayToList(classRequestMapping.method());
            uris = CollectionUtils.arrayToList(classRequestMapping.value());
        }

        // Get the API Operation and see if this endpoint is hidden.
        ApiOperation apiOperation = method.getAnnotation(ApiOperation.class);
        boolean hidden = apiOperation != null && apiOperation.hidden();

        // Only process methods that have a RequestMapping annotation.
        RequestMapping methodRequestMapping = method.getAnnotation(RequestMapping.class);
        if ((methodRequestMapping != null) && (!hidden))
        {
            log.debug("Method \"" + method.getName() + "\" is a RequestMapping.");

            // Override values with method level ones if present.
            requestMethods = getClassOrMethodValue(requestMethods, CollectionUtils.arrayToList(methodRequestMapping.method()));
            uris = getClassOrMethodValue(uris, CollectionUtils.arrayToList(methodRequestMapping.value()));
            produces = getClassOrMethodValue(produces, CollectionUtils.arrayToList(methodRequestMapping.produces()));
            consumes = getClassOrMethodValue(consumes, CollectionUtils.arrayToList(methodRequestMapping.consumes()));

            // Perform validation.
            if (requestMethods.isEmpty())
            {
                log.warn("No request method defined for method \"" + method.getName() + "\". Skipping...");
                return;
            }
            if (uris.isEmpty())
            {
                log.warn("No URI defined for method \"" + method.getName() + "\". Skipping...");
                return;
            }
            if (uris.size() > 1)
            {
                log.warn(uris.size() + " URI's found for method \"" + method.getName() + "\". Only processing the first one.");
            }
            if (requestMethods.size() > 1)
            {
                log.warn(uris.size() + " request methods found for method \"" + method.getName() + "\". Only processing the first one.");
            }

            String uri = uris.get(0).trim();
            Path path = swagger.getPath(uri);
            if (path == null)
            {
                path = new Path();
                swagger.path(uri, path);
            }

            // Get the method summary from the ApiOperation annotation or use the method name if the annotation doesn't exist.
            String methodSummary = method.getName();
            if (apiOperation != null)
            {
                methodSummary = apiOperation.value();
            }

            Operation operation = new Operation();
            operation.tag(tagName);
            operation.summary(methodSummary);

            if (javaDocSource.getText() != null)
            {
                // Process the method description.
                Javadoc javadoc = (Javadoc) javaDocSource.getInternal();
                List<TagElement> tagList = javadoc.tags();
                StringBuilder stringBuilder = new StringBuilder();
                for (TagElement tagElement : tagList)
                {
                    // Tags that have a null tag name are related to the overall method description (as opposed to the individual parameters, etc.).
                    // In most cases, there should be only 1, but perhaps that are other cases that could have more. This general logic comes from
                    // JavaDocImpl.getText(). Although that implementation also filters out on TextElements, we'll grab them all in case there's something
                    // else available (e.g. @link, etc.).
                    if (tagElement.getTagName() == null)
                    {
                        processFragments(tagElement.fragments(), stringBuilder);
                    }
                }

                // The string builder has the final method text to use.
                operation.description(stringBuilder.toString());
                setOperationId(tagName, method, operation);
            }

            if (!produces.isEmpty())
            {
                operation.setProduces(produces);
            }
            if (!consumes.isEmpty())
            {
                operation.setConsumes(consumes);
            }
            path.set(requestMethods.get(0).name().toLowerCase(), operation); // HTTP method MUST be lower cased

            // Process each method parameter.
            // We are using the parameter source here instead of the reflection method's parameters so we can match it to it's Javadoc descriptions.
            // The reflection approach only uses auto-generated parameter names (e.g. arg0, arg1, etc.) which we can't match to Javadoc parameter
            // names.
            for (ParameterSource<JavaClassSource> parameterSource : methodSource.getParameters())
            {
                processRestMethodParameter(parameterSource, operation, methodParamDescriptions);
            }

            // Process the return value.
            processRestMethodReturnValue(method.getReturnType(), operation, methodParamDescriptions.get("@return"));
        }
        else
        {
            log.debug("Skipping method \"" + method.getName() + "\" because it is either not a RequestMapping or it is hidden.");
        }
    }

    /**
     * Processes the Java doc tag (i.e. the parameters and return value).
     *
     * @param javaDocTag the Java doc tag
     * @param methodParamDescriptions the map of method parameters to update.
     */
    private void processJavaDocTag(JavaDocTag javaDocTag, Map<String, String> methodParamDescriptions)
    {
        // Get the list of fragments which are the parts of an individual Javadoc parameter or return value.
        TagElement tagElement = (TagElement) javaDocTag.getInternal();
        List fragments = tagElement.fragments();

        // We need to populate the parameter name and get the list of fragments that contain the actual text.
        String paramName = "";
        List subFragments = new ArrayList<>();
        if (javaDocTag.getName().equals("@param"))
        {
            // In the case of @param, the first fragment is the name and the rest make up the description.
            paramName = String.valueOf(fragments.get(0));
            subFragments = fragments.subList(1, fragments.size());
        }
        else if (javaDocTag.getName().equals("@return"))
        {
            // In the case of @return, we'll use "@return" itself for the name and all the fragments make up the description.
            paramName = "@return";
            subFragments = fragments;
        }

        // Process all fragments and place the results in the map.
        StringBuilder stringBuilder = new StringBuilder();
        processFragments(subFragments, stringBuilder);
        methodParamDescriptions.put(paramName, stringBuilder.toString());
    }

    /**
     * Processes all the fragments that make up the description. This needs to be done manually as opposed to using the higher level roaster text retrieval
     * methods since those eat carriage return characters and don't replace them with a space. The result is the the last word of one line and the first word of
     * the next line are placed together with no separating space. This method builds it manually to fix this issue.
     * <p>
     * This method updates a passed in stringBuilder so callers can process multiple list of fragments and use the same stringBuilder to hold the processed
     * contents of all of them.
     *
     * @param fragments the list of fragments.
     * @param stringBuilder the string builder to update.
     */
    private void processFragments(List fragments, StringBuilder stringBuilder)
    {
        // Loop through the fragments.
        for (Object fragment : fragments)
        {
            // Get and trim this fragment.
            String fragmentString = String.valueOf(fragment).trim();

            // If we have already processed a fragment, add a space.
            if (stringBuilder.length() > 0)
            {
                stringBuilder.append(' ');
            }

            // Append this fragment to the string builder.
            stringBuilder.append(fragmentString);
        }
    }

    /**
     * Sets an operation Id on the operation based on the specified method. The operation Id takes on the format of
     * "~tagNameWithoutSpaces~.~methodName~[~counter~]".
     *
     * @param tagName the tag name for the class.
     * @param method the method for the operation.
     * @param operation the operation to set the Id on.
     */
    private void setOperationId(String tagName, Method method, Operation operation)
    {
        // Initialize the counter and the "base" operation Id (i.e. the one without the counter) and default the operation Id we're going to use to the base
        // one.
        int count = 0;
        String baseOperationId = tagName.replaceAll(" ", "") + "." + method.getName();
        String operationId = baseOperationId;

        // As long as the operation Id is a duplicate with one used before, add a counter to the end of it until we find one that hasn't been used before.
        while (operationIds.contains(operationId))
        {
            count++;
            operationId = baseOperationId + count;
        }

        // Add our new operation Id to the set so we don't use it again and set the operation Id on the operation itself.
        operationIds.add(operationId);
        operation.setOperationId(operationId);
    }

    /**
     * Process a REST method parameter.
     *
     * @param parameterSource the parameter source information.
     * @param operation the Swagger operation.
     * @param methodParamDescriptions the method parameter Javadoc descriptions.
     *
     * @throws MojoExecutionException if any problems were encountered.
     */
    private void processRestMethodParameter(ParameterSource<JavaClassSource> parameterSource, Operation operation, Map<String, String> methodParamDescriptions)
        throws MojoExecutionException
    {
        log.debug("Processing parameter \"" + parameterSource.getName() + "\".");

        try
        {
            AnnotationSource<JavaClassSource> requestParamAnnotationSource = parameterSource.getAnnotation(RequestParam.class);
            AnnotationSource<JavaClassSource> requestBodyAnnotationSource = parameterSource.getAnnotation(RequestBody.class);
            AnnotationSource<JavaClassSource> pathVariableAnnotationSource = parameterSource.getAnnotation(PathVariable.class);

            if (requestParamAnnotationSource != null)
            {
                log.debug("Parameter \"" + parameterSource.getName() + "\" is a RequestParam.");
                QueryParameter queryParameter = new QueryParameter();

                queryParameter.name(requestParamAnnotationSource.getStringValue("value").trim());
                queryParameter.setRequired(BooleanUtils.toBoolean(requestParamAnnotationSource.getStringValue("required")));
                setParameterType(parameterSource, queryParameter);
                operation.parameter(queryParameter);
                setParamDescription(parameterSource, methodParamDescriptions, queryParameter);
            }
            else if (requestBodyAnnotationSource != null)
            {
                log.debug("Parameter \"" + parameterSource.getName() + "\" is a RequestBody.");
                // Add the class name to the list of classes which we will create an example for.
                exampleClassNames.add(parameterSource.getType().getSimpleName());

                BodyParameter bodyParameter = new BodyParameter();
                XmlType xmlType = getXmlType(Class.forName(parameterSource.getType().getQualifiedName()));
                String name = xmlType.name().trim();
                bodyParameter.name(name);
                bodyParameter.setRequired(true);
                bodyParameter.setSchema(new RefModel(name));
                operation.parameter(bodyParameter);
                setParamDescription(parameterSource, methodParamDescriptions, bodyParameter);
            }
            else if (pathVariableAnnotationSource != null)
            {
                log.debug("Parameter \"" + parameterSource.getName() + "\" is a PathVariable.");
                PathParameter pathParameter = new PathParameter();
                pathParameter.name(pathVariableAnnotationSource.getStringValue("value").trim());
                setParameterType(parameterSource, pathParameter);
                operation.parameter(pathParameter);
                setParamDescription(parameterSource, methodParamDescriptions, pathParameter);
            }
        }
        catch (ClassNotFoundException e)
        {
            throw new MojoExecutionException("Unable to instantiate class \"" + parameterSource.getType().getQualifiedName() + "\". Reason: " + e.getMessage(),
                e);
        }
    }

    /**
     * Converts the given Java parameter type into a Swagger param type and sets it into the given Swagger param.
     *
     * @param parameterSource the parameter source.
     * @param swaggerParam the Swagger parameter.
     */
    private void setParameterType(ParameterSource<JavaClassSource> parameterSource, SerializableParameter swaggerParam) throws MojoExecutionException
    {
        try
        {
            String typeName = parameterSource.getType().getQualifiedName();

            if (String.class.getName().equals(typeName))
            {
                swaggerParam.setType("string");
            }
            else if (Integer.class.getName().equals(typeName) || Long.class.getName().equals(typeName))
            {
                swaggerParam.setType("integer");
            }
            else if (Boolean.class.getName().equals(typeName))
            {
                swaggerParam.setType("boolean");
            }
            else
            {
                // See if the type is an enum.
                Enum<?>[] enumValues = (Enum<?>[]) Class.forName(parameterSource.getType().getQualifiedName()).getEnumConstants();
                if (enumValues != null)
                {
                    swaggerParam.setType("string");
                    swaggerParam.setEnum(new ArrayList<>());
                    for (Enum<?> enumEntry : enumValues)
                    {
                        swaggerParam.getEnum().add(enumEntry.name());
                    }
                }
                else
                {
                    // Assume "string" for all other types since everything is ultimately a string.
                    swaggerParam.setType("string");
                }
            }

            log.debug("Parameter \"" + parameterSource.getName() + "\" is a type \"" + swaggerParam.getType() + "\".");
        }
        catch (ClassNotFoundException e)
        {
            throw new MojoExecutionException("Unable to instantiate class \"" + parameterSource.getType().getQualifiedName() + "\". Reason: " + e.getMessage(),
                e);
        }
    }

    /**
     * Sets a Swagger parameter description.
     *
     * @param parameterSource the parameter source information.
     * @param methodParamDescriptions the map of parameter names to their descriptions.
     * @param swaggerParam the Swagger parameter metadata to update.
     */
    private void setParamDescription(ParameterSource<JavaClassSource> parameterSource, Map<String, String> methodParamDescriptions,
        io.swagger.models.parameters.Parameter swaggerParam)
    {
        // Set the parameter description if one was found.
        String parameterDescription = methodParamDescriptions.get(parameterSource.getName());
        log.debug("Parameter \"" + parameterSource.getName() + "\" has description\"" + parameterDescription + "\".");
        if (parameterDescription != null)
        {
            swaggerParam.setDescription(parameterDescription);
        }
    }

    /**
     * Processes the return value of a RequestMapping annotated method.
     *
     * @param returnType the return type.
     * @param operation the operation.
     * @param returnDescription the description of the return value.
     *
     * @throws MojoExecutionException if the return type isn't an XmlType.
     */
    private void processRestMethodReturnValue(Class<?> returnType, Operation operation, String returnDescription) throws MojoExecutionException
    {
        log.debug("Processing REST method return value \"" + returnType.getName() + "\".");

        // Add the class name to the list of classes which we will create an example for.
        exampleClassNames.add(returnType.getSimpleName());

        // Add the success response
        operation.response(200, new Response().description(returnDescription == null ? "Success" : returnDescription)
            .schema(new RefProperty(getXmlType(returnType).name().trim())));

        // If we have an error class, add that as the default response.
        if (modelErrorClass != null)
        {
            operation.defaultResponse(new Response().description("General Error").schema(new RefProperty(getXmlType(modelErrorClass).name().trim())));
        }
    }

    /**
     * Gets the method level object if not empty or uses the class level if the method level is empty.
     *
     * @param classLevel the class level object.
     * @param methodLevel the method level object.
     * @param <T> the type of the object.
     *
     * @return the class or method level object.
     */
    private <T extends List<?>> T getClassOrMethodValue(T classLevel, T methodLevel)
    {
        return (methodLevel == null || methodLevel.isEmpty()) ? classLevel : methodLevel;
    }

    /**
     * Gets the XmlType annotation from the specified class. If the XmlType doesn't exist, an exception will be thrown.
     *
     * @param clazz the class with the XmlType annotation.
     *
     * @return the XmlType.
     * @throws MojoExecutionException if the class isn't an XmlType.
     */
    private XmlType getXmlType(Class<?> clazz) throws MojoExecutionException
    {
        XmlType xmlType = clazz.getAnnotation(XmlType.class);
        if (xmlType == null)
        {
            throw new MojoExecutionException("Class \"" + clazz.getName() + "\" is not of XmlType.");
        }
        return xmlType;
    }

    /**
     * Gets the example class names.
     *
     * @return the example class names.
     */
    public Set<String> getExampleClassNames()
    {
        return exampleClassNames;
    }
}
