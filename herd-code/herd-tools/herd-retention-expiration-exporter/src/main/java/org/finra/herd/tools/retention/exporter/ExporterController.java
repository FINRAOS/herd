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

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.core.helper.HerdThreadHelper;
import org.finra.herd.dao.helper.JsonHelper;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectDataKeys;
import org.finra.herd.model.dto.RegServerAccessParamsDto;
import org.finra.herd.model.dto.RetentionExpirationExporterInputManifestDto;
import org.finra.herd.service.helper.BusinessObjectDataHelper;
import org.finra.herd.service.helper.StorageHelper;
import org.finra.herd.tools.common.databridge.CSVUtils;
import org.finra.herd.tools.common.databridge.DataBridgeController;

/**
 * Executes the ExporterApp workflow.
 */
@Component
public class ExporterController extends DataBridgeController
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ExporterController.class);

    @Autowired
    private BusinessObjectDataHelper businessObjectDataHelper;

    @Autowired
    private ConfigurationHelper configurationHelper;

    @Autowired
    private HerdThreadHelper herdThreadHelper;

    @Autowired
    private JsonHelper jsonHelper;

    @Autowired
    private ExporterManifestReader manifestReader;

    @Autowired
    private StorageHelper storageHelper;

    @Autowired
    private ExporterWebClient exporterWebClient;

    /**
     * Executes the retention expiration exporter workflow.
     *
     * @param regServerAccessParamsDto the DTO for the parameters required to communicate with the registration server
     * @param manifestPath the local path to the manifest file
     * @param namespace the namespace of business object data
     * @param businessObjectDefinitionName the business object definition name of business object data
     * @param force if set, allows upload to proceed when the latest version of the business object data has UPLOADING status by invalidating that version
     * @param maxRetryAttempts the maximum number of the business object data registration retry attempts
     * @param retryDelaySecs the delay in seconds between the business object data registration retry attempts
     *
     * @throws InterruptedException if the upload thread was interrupted.
     * @throws JAXBException if a JAXB error was encountered.
     * @throws IOException if an I/O error was encountered.
     * @throws URISyntaxException if a URI syntax error was encountered.
     */
    @SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST_OF_RETURN_VALUE", justification = "manifestReader.readJsonManifest will always return an RetentionExpirationExporterInputManifestDto object.")
    public void performRetentionExpirationExport(RegServerAccessParamsDto regServerAccessParamsDto, File manifestPath, String namespace,
        String businessObjectDefinitionName, Boolean force, Integer maxRetryAttempts, Integer retryDelaySecs)
        throws InterruptedException, JAXBException, IOException, URISyntaxException
    {
        BusinessObjectDataKeys businessObjectDataKeys = null;

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
            String csvFile = "/Users/mkyong/csv/abc.csv";
            FileWriter writer = new FileWriter(csvFile);

            for (BusinessObjectDataKey businessObjectDataKey : businessObjectDataKeys.getBusinessObjectDataKeys())
            {

                List<String> bDataArray = Arrays.asList(businessObjectDataKey.getNamespace(), businessObjectDataKey.getBusinessObjectFormatUsage(),
                    businessObjectDataKey.getBusinessObjectFormatFileType(), businessObjectDataKey.getBusinessObjectFormatVersion().toString(),
                    businessObjectDataKey.getPartitionValue(), businessObjectDataKey.getSubPartitionValues().get(0),
                    businessObjectDataKey.getSubPartitionValues().get(1), businessObjectDataKey.getSubPartitionValues().get(2),
                    businessObjectDataKey.getSubPartitionValues().get(3), businessObjectDataKey.getBusinessObjectDataVersion().toString());
                CSVUtils.writeLine(writer, bDataArray, ',', '"');
                LOGGER.info("BusinessObjectDataKey|" + bDataArray.toString());
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
