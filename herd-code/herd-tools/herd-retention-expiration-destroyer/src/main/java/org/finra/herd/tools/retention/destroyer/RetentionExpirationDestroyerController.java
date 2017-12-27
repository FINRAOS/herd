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
package org.finra.herd.tools.retention.destroyer;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.opencsv.CSVReader;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.dao.helper.JsonHelper;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.dto.RegServerAccessParamsDto;

@Component
public class RetentionExpirationDestroyerController
{
    private static final String[] CSV_FILE_HEADER_COLUMNS =
        {"Namespace", "Business Object Definition Name", "Business Object Format Usage", "Business Object Format File Type", "Business Object Format Version",
            "Primary Partition Value", "Sub-Partition Value 1", "Sub-Partition Value 2", "Sub-Partition Value 3", "Sub-Partition Value 4",
            "Business Object Data Version", "Business Object Definition Display Name", "Business Object Definition URI"};

    private static final Logger LOGGER = LoggerFactory.getLogger(RetentionExpirationDestroyerController.class);

    @Autowired
    private JsonHelper jsonHelper;

    @Autowired
    private RetentionExpirationDestroyerWebClient retentionExpirationDestroyerWebClient;

    /**
     * Executes the retention expiration destroyer workflow.
     *
     * @param localInputFile the local input file
     * @param regServerAccessParamsDto the DTO for the parameters required to communicate with the registration server
     *
     * @throws Exception if any problems were encountered
     */
    public void performRetentionExpirationDestruction(File localInputFile, RegServerAccessParamsDto regServerAccessParamsDto) throws Exception
    {
        // Read business object data keys from the input CSV file.
        List<BusinessObjectDataKey> businessObjectDataKeys = getBusinessObjectDataKeys(localInputFile);

        // Initialize the web client.
        retentionExpirationDestroyerWebClient.setRegServerAccessParamsDto(regServerAccessParamsDto);

        // Process business object data keys one by one.
        LOGGER.info("Processing {} business object data instances for destruction.", CollectionUtils.size(businessObjectDataKeys));
        for (BusinessObjectDataKey businessObjectDataKey : businessObjectDataKeys)
        {
            retentionExpirationDestroyerWebClient.destroyBusinessObjectData(businessObjectDataKey);
            LOGGER.info("Successfully marked for destruction. Business object data {}", jsonHelper.objectToJson(businessObjectDataKey));
        }

        LOGGER.info("Successfully processed {} business object data instances for destruction.", CollectionUtils.size(businessObjectDataKeys));
    }

    /**
     * Extracts business object data key from a CSV file line. This method also validates the format of the line.
     *
     * @param line the input line
     * @param lineNumber the input line number
     * @param inputCsvFile the input CSV file
     *
     * @return the business object data key
     */
    protected BusinessObjectDataKey getBusinessObjectDataKey(String[] line, int lineNumber, File inputCsvFile)
    {
        if (line.length != CSV_FILE_HEADER_COLUMNS.length)
        {
            throw new IllegalArgumentException(
                String.format("Line number %d of input file \"%s\" does not match the expected format.", lineNumber, inputCsvFile.toString()));
        }

        Integer businessObjectFormatVersion;
        Integer businessObjectDataVersion;

        try
        {
            businessObjectFormatVersion = Integer.valueOf(line[4]);
        }
        catch (NumberFormatException e)
        {
            throw new IllegalArgumentException(String
                .format("Line number %d of input file \"%s\" does not match the expected format. Business object format version must be an integer.",
                    lineNumber, inputCsvFile.toString()), e);
        }

        try
        {
            businessObjectDataVersion = Integer.valueOf(line[10]);
        }
        catch (NumberFormatException e)
        {
            throw new IllegalArgumentException(String
                .format("Line number %d of input file \"%s\" does not match the expected format. Business object data version must be an integer.", lineNumber,
                    inputCsvFile.toString()), e);
        }

        // Build a list of optional sub-partition values.
        List<String> subPartitionValues = new ArrayList<>();
        for (String subPartitionValue : Arrays.asList(line[6], line[7], line[8], line[9]))
        {
            if (StringUtils.isNotBlank(subPartitionValue))
            {
                subPartitionValues.add(subPartitionValue);
            }
            else
            {
                break;
            }
        }

        return new BusinessObjectDataKey(line[0], line[1], line[2], line[3], businessObjectFormatVersion, line[5], subPartitionValues,
            businessObjectDataVersion);
    }

    /**
     * Get business object data keys from the input CSV tile. This method also validates the input file format.
     *
     * @param inputCsvFile the input CSV file
     *
     * @return the list of business object data keys
     * @throws IOException if any problems were encountered
     */
    protected List<BusinessObjectDataKey> getBusinessObjectDataKeys(File inputCsvFile) throws IOException
    {
        List<BusinessObjectDataKey> businessObjectDataKeyList = new ArrayList<>();

        // Read the input CSV file and populate business object data key list.
        try (CSVReader csvReader = new CSVReader(new InputStreamReader(new FileInputStream(inputCsvFile), StandardCharsets.UTF_8)))
        {
            String[] line;

            // Validate required header of the CSV input file.
            if ((line = csvReader.readNext()) == null || !Arrays.equals(line, CSV_FILE_HEADER_COLUMNS))
            {
                throw new IllegalArgumentException(String.format("Input file \"%s\" does not contain the expected CSV file header.", inputCsvFile.toString()));
            }

            // Process the input CSV file line by line.
            int lineCount = 2;
            while ((line = csvReader.readNext()) != null)
            {
                businessObjectDataKeyList.add(getBusinessObjectDataKey(line, lineCount++, inputCsvFile));
            }
        }

        return businessObjectDataKeyList;
    }
}
