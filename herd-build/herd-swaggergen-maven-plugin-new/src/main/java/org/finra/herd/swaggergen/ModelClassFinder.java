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
import java.util.HashSet;
import java.util.Set;

import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.logging.Log;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.core.type.classreading.CachingMetadataReaderFactory;
import org.springframework.core.type.classreading.MetadataReader;
import org.springframework.util.ClassUtils;
import org.springframework.util.SystemPropertyUtils;

/**
 * Class that finds the model classes.
 */
public class ModelClassFinder
{
    // The log to use for logging purposes.
    @SuppressWarnings("PMD.ProperLogger") // Logger is passed into this method from Mojo base class.
    private Log log;

    // The model classes.
    private Set<Class<?>> modelClasses;

    // The model error class.
    private Class<?> modelErrorClass;

    // The Java package where the model classes reside.
    private String modelJavaPackage;

    // The class name of the error model class.
    private String modelErrorClassName;

    /**
     * Constructs the model class finder.
     *
     * @param log the log.
     * @param modelJavaPackage the model Java package.
     * @param modelErrorClassName the model error class name.
     *
     * @throws MojoExecutionException if any problems were encountered.
     */
    public ModelClassFinder(Log log, String modelJavaPackage, String modelErrorClassName) throws MojoExecutionException
    {
        this.log = log;
        this.modelJavaPackage = modelJavaPackage;
        this.modelErrorClassName = modelErrorClassName;

        findModelClasses();
    }

    /**
     * Finds all the model classes and the model error class.
     *
     * @throws MojoExecutionException if a class couldn't be instantiated or an I/O error occurred.
     */
    private void findModelClasses() throws MojoExecutionException
    {
        try
        {
            log.debug("Finding model classes.");

            // Get the model classes as resources.
            modelClasses = new HashSet<>();

            // Loop through each model resource and add each one to the set of model classes.
            for (Resource resource : ResourceUtils.getResources(ResourcePatternResolver.CLASSPATH_ALL_URL_PREFIX +
                ClassUtils.convertClassNameToResourcePath(SystemPropertyUtils.resolvePlaceholders(modelJavaPackage)) +
                "/**/*.class"))
            {
                if (resource.isReadable())
                {
                    MetadataReader metadataReader = new CachingMetadataReaderFactory(new PathMatchingResourcePatternResolver()).getMetadataReader(resource);
                    Class<?> clazz = Class.forName(metadataReader.getClassMetadata().getClassName());
                    modelClasses.add(clazz);
                    log.debug("Found model class \"" + clazz.getName() + "\".");

                    // If the model error class name is configured and matches this class, then hold onto it.
                    if (clazz.getSimpleName().equals(modelErrorClassName))
                    {
                        log.debug("Found model error class \"" + clazz.getName() + "\".");
                        modelErrorClass = clazz;
                    }
                }
            }
        }
        catch (IOException | ClassNotFoundException e)
        {
            throw new MojoExecutionException("Error finding model classes. Reason: " + e.getMessage(), e);
        }
    }

    /**
     * Gets the model classes.
     *
     * @return the set of model classes (e.g. parameters, return types, error information, etc.).
     */
    public Set<Class<?>> getModelClasses()
    {
        return modelClasses;
    }

    /**
     * Gets the model error class.
     *
     * @return the model error class.
     */
    public Class<?> getModelErrorClass()
    {
        return modelErrorClass;
    }
}
