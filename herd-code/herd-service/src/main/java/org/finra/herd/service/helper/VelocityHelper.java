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
package org.finra.herd.service.helper;

import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Map;

import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;
import org.apache.velocity.runtime.RuntimeConstants;
import org.apache.velocity.util.introspection.SecureUberspector;
import org.springframework.stereotype.Component;

/**
 * A helper to abstract operations around Apache Velocity.
 * Note: This is the strict version of the velocity helper.
 */
@Component
public class VelocityHelper
{
    /**
     * Initializes the Velocity engine.
     */
    public VelocityHelper()
    {
        Velocity.setProperty(RuntimeConstants.RUNTIME_REFERENCES_STRICT, true);
        Velocity.setProperty(RuntimeConstants.UBERSPECT_CLASSNAME, SecureUberspector.class.getName());
        Velocity.init();
    }

    /**
     * Wrapper for {@link Velocity#evaluate(org.apache.velocity.context.Context, java.io.Writer, String, java.io.InputStream)}
     *
     * @param templateReader A {@link Reader} of a Velocity template.
     * @param variables Variables to add to context
     * @param logTag The log tag
     *
     * @return {@link String} result of evaluation
     */
    public String evaluate(Reader templateReader, Map<String, Object> variables, String logTag)
    {
        VelocityContext velocityContext = new VelocityContext(variables);
        StringWriter writer = new StringWriter();
        if (!Velocity.evaluate(velocityContext, writer, logTag, templateReader))
        {
            // Although the Velocity.evaluate method's Javadoc states that it will return false to indicate a failure when the template couldn't be
            // processed and to see the Velocity log messages for more details, upon examining the method's implementation, it doesn't look like
            // it will ever return false. Instead, other RuntimeExceptions will be thrown (e.g. ParseErrorException).
            // Nonetheless, we'll leave this checking here to honor the method's contract in case the implementation changes in the future.
            // Having said that, there will be no way to JUnit test this flow.
            throw new IllegalStateException("Error evaluating velocity template. See velocity log message for more details.");
        }
        return writer.toString();
    }

    /**
     * Wrapper for {@link Velocity#evaluate(org.apache.velocity.context.Context, java.io.Writer, String, java.io.InputStream)}
     *
     * @param template The template {@link String}
     * @param variables Variables to add to context
     * @param logTag The log tag
     *
     * @return {@link String} result of evaluation
     */
    public String evaluate(String template, Map<String, Object> variables, String logTag)
    {
        StringReader templateReader = new StringReader(template);
        return evaluate(templateReader, variables, logTag);
    }
}