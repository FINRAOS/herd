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
import java.nio.file.Path;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.commons.io.IOUtils;
import org.apache.maven.monitor.logging.DefaultLog;
import org.codehaus.plexus.logging.AbstractLogger;
import org.codehaus.plexus.logging.console.ConsoleLogger;

public abstract class AbstractTest
{
    protected static final DefaultLog LOG;

    static
    {
        ConsoleLogger logger = new ConsoleLogger();
        logger.setThreshold(AbstractLogger.LEVEL_ERROR);
        LOG = new DefaultLog(logger);
    }

    protected String toYaml(Object object) throws JsonProcessingException
    {
        ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());
        objectMapper.setSerializationInclusion(Include.NON_EMPTY);
        objectMapper.setSerializationInclusion(Include.NON_NULL);
        return objectMapper.writeValueAsString(object);
    }

    protected InputStream getResource(String name)
    {
        return getClass().getResourceAsStream(name);
    }

    protected String getResourceAsString(String name) throws IOException
    {
        return replaceLineSeparators(IOUtils.toString(getClass().getResourceAsStream(name)));
    }

    protected String getFileAsString(Path path) throws IOException
    {
        return replaceLineSeparators(IOUtils.toString(path.toUri()));
    }

    private String replaceLineSeparators(String content)
    {
        return content.replace(System.lineSeparator(), "\n");
    }
}
