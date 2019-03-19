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
package org.finra.herd.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.springframework.stereotype.Component;

import org.finra.herd.model.api.xml.CustomDdl;
import org.finra.herd.model.api.xml.CustomDdlCreateRequest;
import org.finra.herd.model.api.xml.CustomDdlKey;
import org.finra.herd.model.api.xml.CustomDdlUpdateRequest;

@Component
public class CustomDdlServiceTestHelper
{
    /**
     * Creates a custom DDL create request.
     *
     * @return the newly created custom DDL create request
     */
    public CustomDdlCreateRequest createCustomDdlCreateRequest(String namespaceCode, String businessObjectDefinitionName, String businessObjectFormatUsage,
        String businessObjectFormatFileType, Integer businessObjectFormatVersion, String customDdlName, String ddl)
    {
        CustomDdlCreateRequest request = new CustomDdlCreateRequest();
        request.setCustomDdlKey(
            new CustomDdlKey(namespaceCode, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType, businessObjectFormatVersion,
                customDdlName));
        request.setDdl(ddl);
        return request;
    }

    /**
     * Creates a custom DDL update request.
     *
     * @return the newly created custom DDL update request
     */
    public CustomDdlUpdateRequest createCustomDdlUpdateRequest(String ddl)
    {
        CustomDdlUpdateRequest request = new CustomDdlUpdateRequest();
        request.setDdl(ddl);
        return request;
    }

    /**
     * Returns the Hive custom DDL.
     *
     * @param partitioned specifies whether the table the custom DDL is for is partitioned or not
     *
     * @return the custom Hive DDL
     */
    public String getTestCustomDdl(boolean partitioned)
    {
        StringBuilder sb = new StringBuilder();

        sb.append("CREATE EXTERNAL TABLE IF NOT EXISTS `${table.name}` (\n");
        sb.append("    `COLUMN001` TINYINT,\n");
        sb.append("    `COLUMN002` SMALLINT COMMENT 'This is \\'COLUMN002\\' column. ");
        sb.append("Here are \\'single\\' and \"double\" quotes along with a backslash \\.',\n");
        sb.append("    `COLUMN003` INT,\n");
        sb.append("    `COLUMN004` BIGINT,\n");
        sb.append("    `COLUMN005` FLOAT,\n");
        sb.append("    `COLUMN006` DOUBLE,\n");
        sb.append("    `COLUMN007` DECIMAL,\n");
        sb.append("    `COLUMN008` DECIMAL(p,s),\n");
        sb.append("    `COLUMN009` DECIMAL,\n");
        sb.append("    `COLUMN010` DECIMAL(p),\n");
        sb.append("    `COLUMN011` DECIMAL(p,s),\n");
        sb.append("    `COLUMN012` TIMESTAMP,\n");
        sb.append("    `COLUMN013` DATE,\n");
        sb.append("    `COLUMN014` STRING,\n");
        sb.append("    `COLUMN015` VARCHAR(n),\n");
        sb.append("    `COLUMN016` VARCHAR(n),\n");
        sb.append("    `COLUMN017` CHAR(n),\n");
        sb.append("    `COLUMN018` BOOLEAN,\n");
        sb.append("    `COLUMN019` BINARY,\n");
        sb.append("    `COLUMN020` ARRAY<BIGINT>,\n");
        sb.append("    `COLUMN021` ARRAY<INT(5)>,\n");
        sb.append("    `COLUMN022` MAP<INT,ARRAY<BIGINT>>)\n");

        if (partitioned)
        {
            sb.append("PARTITIONED BY (`PRTN_CLMN001` DATE, `PRTN_CLMN002` STRING, `PRTN_CLMN003` INT, `PRTN_CLMN004` DECIMAL, " +
                "`PRTN_CLMN005` BOOLEAN, `PRTN_CLMN006` DECIMAL, `PRTN_CLMN007` DECIMAL)\n");
        }

        sb.append("ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' ESCAPED BY '\\\\' COLLECTION ITEMS TERMINATED BY ',' MAP KEYS TERMINATED BY '#' " +
            "NULL DEFINED AS '\\N'\n");

        if (partitioned)
        {
            sb.append("STORED AS TEXTFILE;");
        }
        else
        {
            sb.append("STORED AS TEXTFILE\n");
            sb.append("LOCATION '${non-partitioned.table.location}';");
        }

        return sb.toString();
    }

    /**
     * Validates custom DDL contents against specified parameters.
     *
     * @param customDdlId the expected custom DDL ID
     * @param expectedNamespace the expected namespace
     * @param expectedBusinessObjectDefinitionName the expected business object definition name
     * @param expectedBusinessObjectFormatUsage the expected business object format usage
     * @param expectedBusinessObjectFormatFileType the expected business object format file type
     * @param expectedBusinessObjectFormatVersion the expected business object format version
     * @param expectedCustomDdlName the expected custom DDL name
     * @param expectedDdl the expected DDL
     * @param actualCustomDdl the custom DDL object instance to be validated
     */
    public void validateCustomDdl(Integer customDdlId, String expectedNamespace, String expectedBusinessObjectDefinitionName,
        String expectedBusinessObjectFormatUsage, String expectedBusinessObjectFormatFileType, Integer expectedBusinessObjectFormatVersion,
        String expectedCustomDdlName, String expectedDdl, CustomDdl actualCustomDdl)
    {
        assertNotNull(actualCustomDdl);
        if (customDdlId != null)
        {
            assertEquals(customDdlId, Integer.valueOf(actualCustomDdl.getId()));
        }
        assertEquals(
            new CustomDdlKey(expectedNamespace, expectedBusinessObjectDefinitionName, expectedBusinessObjectFormatUsage, expectedBusinessObjectFormatFileType,
                expectedBusinessObjectFormatVersion, expectedCustomDdlName), actualCustomDdl.getCustomDdlKey());
        assertEquals(expectedDdl, actualCustomDdl.getDdl());
    }
}
