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
package org.finra.herd.tools.retention.exporter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;

import javax.xml.bind.JAXBException;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectDataKeys;
import org.finra.herd.model.dto.RegServerAccessParamsDto;
import org.finra.herd.model.dto.RetentionExpirationExporterInputManifestDto;

/**
 * Executes the ExporterApp workflow.
 */
@Component
public class ExporterController
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ExporterController.class);

    @Autowired
    private ExporterWebClient exporterWebClient;

    /**
     * Executes the retention expiration exporter workflow.
     *
     * @param namespace the namespace of business object data
     * @param businessObjectDefinitionName the business object definition name of business object data
     * @param localOutputFile the local output file
     * @param regServerAccessParamsDto the DTO for the parameters required to communicate with the registration server
     *
     * @throws Exception if any problems were encountered
     */
    @SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST_OF_RETURN_VALUE",
        justification = "manifestReader.readJsonManifest will always return an RetentionExpirationExporterInputManifestDto object.")
    public void performRetentionExpirationExport(String namespace, String businessObjectDefinitionName, File localOutputFile,
        RegServerAccessParamsDto regServerAccessParamsDto) throws Exception
    {
        BusinessObjectDataKeys businessObjectDataKeys;

        try
        {
            // Process manifest file
            RetentionExpirationExporterInputManifestDto manifest = new RetentionExpirationExporterInputManifestDto();
            manifest.setNamespace(namespace);
            manifest.setBusinessObjectDefinitionName(businessObjectDefinitionName);

            // Initialize retention expiration exporter web client.
            exporterWebClient.setRegServerAccessParamsDto(regServerAccessParamsDto);

            // Get business object data keys.
            businessObjectDataKeys = exporterWebClient.getBusinessObjectDataKeys(manifest);

            // Writing business object data to the CSV file
            FileWriter writer = new FileWriter(localOutputFile);

            for (BusinessObjectDataKey businessObjectDataKey : businessObjectDataKeys.getBusinessObjectDataKeys())
            {
                List<String> businessObjectDataRecords = Arrays
                    .asList(businessObjectDataKey.getNamespace(), businessObjectDataKey.getBusinessObjectFormatUsage(),
                        businessObjectDataKey.getBusinessObjectFormatFileType(), businessObjectDataKey.getBusinessObjectFormatVersion().toString(),
                        businessObjectDataKey.getPartitionValue(), businessObjectDataKey.getSubPartitionValues().get(0),
                        businessObjectDataKey.getSubPartitionValues().get(1), businessObjectDataKey.getSubPartitionValues().get(2),
                        businessObjectDataKey.getSubPartitionValues().get(3), businessObjectDataKey.getBusinessObjectDataVersion().toString());
                //CSVUtils.writeLine(writer, bDataArray, ',', '"');
                LOGGER.info("BusinessObjectDataKey|" + businessObjectDataRecords.toString());
            }

            writer.flush();
            writer.close();
        }
        catch (JAXBException | IOException | URISyntaxException e)
        {
            throw e;
        }
    }
}
