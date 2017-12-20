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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.amazonaws.services.simplesystemsmanagement.model.InvalidAllowedPatternException;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.dto.RegServerAccessParamsDto;

/**
 * Executes the RetentionExpirationDestroyerController workflow.
 */
@Component
public class RetentionExpirationDestroyerController
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RetentionExpirationDestroyerController.class);

    private static final String CSV_FILE_HEADER =
        "\"Namespace\",\"Business Object Definition Name\",\"Business Object Format Usage\",\"Business Object Format File Type\"," +
            "\"Business Object Format Version\",\"Primary Partition Value\",\"Sub-Partition Value 1\",\"Sub-Partition Value 2\",\"Sub-Partition Value 3\"," +
            "\"Sub-Partition Value 4\",\"Business Object Data Version\",\"Business Object Definition Display Name\",\"Business Object Definition URI\"";

    @Autowired
    private RetentionExpirationDestroyerWebClient destructorWebClient;

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
        destructorWebClient.setRegServerAccessParamsDto(regServerAccessParamsDto);

        // Process business object data keys one by one.
        LOGGER.info("Processing {} business object data keys for destruction.", CollectionUtils.size(businessObjectDataKeys));
        for (BusinessObjectDataKey businessObjectDataKey : businessObjectDataKeys)
        {
            destructorWebClient.destroyBusinessObjectData(businessObjectDataKey);
            LOGGER.info("Marked for destruction. Business object data {}", businessObjectDataKey.toString());
        }

        LOGGER.info("All Business object data keys are processed for destruction.");
    }

    /**
     * Get business object data keys from input tile. This method also validates input file format.
     *
     * @param csvFile the input csv file
     *
     * @return the list of business object data key
     * @throws IOException if any problems were encountered
     */
    protected List<BusinessObjectDataKey> getBusinessObjectDataKeys(File csvFile) throws IOException
    {
        List<BusinessObjectDataKey> businessObjectDataKeyList = new ArrayList<>();
        String line = "";

        // Read the input CSV file and populate business object data key list.
        try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(csvFile), StandardCharsets.UTF_8)))
        {
            // Validate header of the csv file.
            if ((line = bufferedReader.readLine()) == null || !line.equals(CSV_FILE_HEADER))
            {
                throw new IllegalArgumentException(String.format("Input CSV file \"%s\" does not have an expected header.", csvFile.toString()));
            }

            // Process CSV file.
            int lineCount = 1;
            while ((line = bufferedReader.readLine()) != null)
            {
                businessObjectDataKeyList.add(getBusinessObjectDataKey(line, lineCount++));
            }
        }

        return businessObjectDataKeyList;
    }

    /**
     * Removes optional double quotes and undo the CSV formatting.
     *
     * @param input the string
     *
     * @return the formatted value
     */
    private String removeOptionalDoubleQuotes(String input)
    {
        String result = input;

        int size = StringUtils.length(result);
        if (size > 1 && result.startsWith("\"") && result.endsWith("\""))
        {
            result = result.substring(1, size - 1);
        }

        return result.replace("\\\"", "\"");
    }

    /**
     * Extracts business object data key. This method also validates CSV format.
     *
     * @param line the input line
     * @param lineNumber the input line number
     *
     * @return the business object data key
     */
    protected BusinessObjectDataKey getBusinessObjectDataKey(String line, int lineNumber)
    {
        if (line.matches("^(\\\\d)+,[A-Za-z]+(,[A-Za-z]+=[A-Za-z0-9{};]+)+$"))
        {
            throw new InvalidAllowedPatternException(String.format("Line number %d does not match the expected format.", lineNumber));
        }

        String[] tokens = line.split(",");

        return new BusinessObjectDataKey(removeOptionalDoubleQuotes(tokens[0]), removeOptionalDoubleQuotes(tokens[1]), removeOptionalDoubleQuotes(tokens[2]),
            removeOptionalDoubleQuotes(tokens[3]), Integer.parseInt(removeOptionalDoubleQuotes(tokens[4])), removeOptionalDoubleQuotes(tokens[5]), Arrays
            .asList(removeOptionalDoubleQuotes(tokens[6]), removeOptionalDoubleQuotes(tokens[7]), removeOptionalDoubleQuotes(tokens[8]),
                removeOptionalDoubleQuotes(tokens[9])), Integer.parseInt(removeOptionalDoubleQuotes(tokens[10])));
    }
}
