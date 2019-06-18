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
package org.finra.herd.model.jpa;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * A file type.
 */
@Table(name = "file_type_cd_lk")
@Entity
public class FileTypeEntity extends AuditableEntity
{
    /**
     * File type for Bzip2 compressed text files.
     */
    public static final String BZ_FILE_TYPE = "BZ";

    /**
     * File type for Gzip compressed text files.
     */
    public static final String GZ_FILE_TYPE = "GZ";

    /**
     * File type for Optimized Row Columnar files.
     */
    public static final String ORC_FILE_TYPE = "ORC";

    /**
     * File type for Parquet files.
     */
    public static final String PARQUET_FILE_TYPE = "PARQUET";

    /**
     * File type for plain text files.
     */
    public static final String TXT_FILE_TYPE = "TXT";

    /**
     * File type for JSON files.
     */
    public static final String JSON_FILE_TYPE = "JSON";

    /**
     * File type for relational tables.
     */
    public static final String RELATIONAL_TABLE_FILE_TYPE = "RELATIONAL_TABLE";

    /**
     * The code column.
     */
    @Id
    @Column(name = "file_type_cd")
    private String code;

    @Column(name = "file_type_ds")
    private String description;

    public String getCode()
    {
        return code;
    }

    public void setCode(String code)
    {
        this.code = code;
    }

    public String getDescription()
    {
        return description;
    }

    public void setDescription(String description)
    {
        this.description = description;
    }
}
