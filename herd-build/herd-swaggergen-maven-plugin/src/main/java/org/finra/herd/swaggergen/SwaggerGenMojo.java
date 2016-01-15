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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.swagger.models.Info;
import io.swagger.models.Scheme;
import io.swagger.models.Swagger;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.springframework.util.CollectionUtils;

/**
 * Goal which generates a Swagger YAML documentation file.
 */
@Mojo(name = "herd-swaggergen", defaultPhase = LifecyclePhase.GENERATE_RESOURCES)
public class SwaggerGenMojo extends AbstractMojo
{
    /**
     * Location where the generated output YAML file will go.
     */
    @org.apache.maven.plugins.annotations.Parameter(property = "outputDir", required = true,
        defaultValue = "${project.build.directory}/generated-sources/herd-swaggergen")
    private File outputDirectory;

    /**
     * The output file name.
     */
    @org.apache.maven.plugins.annotations.Parameter(property = "outputFilename", required = true, defaultValue = "swagger.yaml")
    private String outputFilename;

    /**
     * The REST Java package location.
     */
    @org.apache.maven.plugins.annotations.Parameter(property = "restJavaPackage", required = true)
    private String restJavaPackage;

    /**
     * The Model Java package location.
     */
    @org.apache.maven.plugins.annotations.Parameter(property = "modelJavaPackage", required = true)
    private String modelJavaPackage;

    /**
     * The Error Information model class name.
     */
    @org.apache.maven.plugins.annotations.Parameter(property = "modelErrorClassName")
    private String modelErrorClassName;

    /**
     * The REST base path. The default assumes the controller's class' names are in the form {TAGNAME}RestController (e.g. SampleRestController). "tag" is the
     * pattern that is read for the value to use.
     */
    @org.apache.maven.plugins.annotations.Parameter(property = "tagPattern", required = true, defaultValue = "(?<tag>.+?)RestController")
    private String tagPatternParameter;

    /**
     * The application title.
     */
    @org.apache.maven.plugins.annotations.Parameter(property = "title", required = true, defaultValue = "Application")
    private String title;

    /**
     * The application version.
     */
    @org.apache.maven.plugins.annotations.Parameter(property = "version", required = true, defaultValue = "${project.version}")
    private String version;

    /**
     * The REST base path.
     */
    @org.apache.maven.plugins.annotations.Parameter(property = "basePath", required = true, defaultValue = "/rest")
    private String basePath;

    /**
     * The list of schemes.
     *
     * @see Scheme
     */
    @org.apache.maven.plugins.annotations.Parameter(property = "schemes")
    private List<String> schemeParameters;

    /**
     * The name of the XSD file which defines the models.
     */
    @org.apache.maven.plugins.annotations.Parameter(property = "xsdName", required = false)
    private String xsdName;

    /**
     * The main execution method for this Mojo.
     *
     * @throws MojoExecutionException if any errors were encountered.
     */
    @Override
    public void execute() throws MojoExecutionException
    {
        // Create the output directory if it doesn't already exist.
        getLog().debug("Creating output directory \"" + outputDirectory + "\".");
        java.nio.file.Path outputDirectoryPath = Paths.get(outputDirectory.toURI());
        if (!Files.exists(outputDirectoryPath))
        {
            try
            {
                Files.createDirectories(outputDirectoryPath);
            }
            catch (IOException e)
            {
                throw new MojoExecutionException("Unable to create directory for output path \"" + outputDirectoryPath + "\".", e);
            }
        }

        // Get a new Swagger metadata object.
        Swagger swagger = getSwagger();

        // Find all the model classes.
        // Note: this needs to be done before we process the REST controllers below because it finds the modelErrorClass.
        ModelClassFinder modelClassFinder = new ModelClassFinder(getLog(), modelJavaPackage, modelErrorClassName);

        // Find and process all the REST controllers and add them to the Swagger metadata.
        RestControllerProcessor restControllerProcessor =
            new RestControllerProcessor(getLog(), swagger, restJavaPackage, tagPatternParameter, modelClassFinder.getModelErrorClass());

        XsdParser xsdParser = null;
        if (xsdName != null)
        {
            xsdParser = new XsdParser(xsdName);
        }

        // Generate the definitions into Swagger based on the model classes collected.
        new DefinitionGenerator(getLog(), swagger, restControllerProcessor.getExampleClassNames(), modelClassFinder.getModelClasses(), xsdParser);

        // Write to Swagger information to a YAML file.
        createYamlFile(swagger);
    }

    /**
     * Gets a new Swagger metadata.
     *
     * @return the Swagger metadata.
     * @throws MojoExecutionException if any problems were encountered.
     */
    private Swagger getSwagger() throws MojoExecutionException
    {
        getLog().debug("Creating Swagger Metadata");
        // Set up initial Swagger metadata.
        Swagger swagger = new Swagger();
        swagger.setInfo(new Info().title(title).version(version));
        swagger.setBasePath(basePath);

        // Set the schemes.
        if (!CollectionUtils.isEmpty(schemeParameters))
        {
            List<Scheme> schemes = new ArrayList<>();
            for (String schemeParameter : schemeParameters)
            {
                Scheme scheme = Scheme.forValue(schemeParameter);
                if (scheme == null)
                {
                    throw new MojoExecutionException("Invalid scheme specified: " + schemeParameter);
                }
                schemes.add(scheme);
            }
            swagger.setSchemes(schemes);
        }

        // Use default paths and definitions.
        swagger.setPaths(new TreeMap<>());
        swagger.setDefinitions(new TreeMap<>());
        return swagger;
    }

    /**
     * Creates the YAML file in the output location based on the Swagger metadata.
     *
     * @param swagger the Swagger metadata.
     *
     * @throws MojoExecutionException if any error was encountered while writing the YAML information to the file.
     */
    private void createYamlFile(Swagger swagger) throws MojoExecutionException
    {
        String yamlOutputLocation = outputDirectory + "/" + outputFilename;
        try
        {
            getLog().debug("Creating output YAML file \"" + yamlOutputLocation + "\"");

            ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());
            objectMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
            objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
            objectMapper.writeValue(new File(yamlOutputLocation), swagger);
        }
        catch (IOException e)
        {
            throw new MojoExecutionException("Error creating output YAML file \"" + yamlOutputLocation + "\". Reason: " + e.getMessage(), e);
        }
    }
}
