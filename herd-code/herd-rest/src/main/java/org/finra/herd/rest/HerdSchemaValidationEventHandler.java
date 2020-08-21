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

package org.finra.herd.rest;

import javax.xml.bind.ValidationEvent;
import javax.xml.bind.ValidationEventHandler;
import javax.xml.bind.util.ValidationEventCollector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A basic event handler for herd schema validation events which is pluggable in the JAXB marshaller/unmarshaller provider. This is needed because parse errors
 * encountered in custom xml type adapters are not propagated and the JAXB marshaller/unmarshaller simply moves on to the next node. Having a custom validation
 * event handler allows us to expose validation errors which herd's error handler: {@link org.finra.herd.service.helper.HerdErrorInformationExceptionHandler}
 * can then process.
 */
public class HerdSchemaValidationEventHandler extends ValidationEventCollector implements ValidationEventHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(HerdSchemaValidationEventHandler.class);

    @Override
    public boolean handleEvent(final ValidationEvent event)
    {
        if (event == null)
        {
            // Ideally, this should never happen but in case validation event is null there is no further
            // information we can propagate so provide a generic message.
            throw new IllegalArgumentException("Parse exception.");
        }

        if (event.getSeverity() == ValidationEvent.WARNING)
        {
            LOGGER.warn("A validation event warning was recorded.");
        }

        if (event.getSeverity() == ValidationEvent.ERROR || event.getSeverity() == ValidationEvent.FATAL_ERROR)
        {
            throw new IllegalArgumentException(event.getMessage(), event.getLinkedException());
        }

        return false;
    }
}
