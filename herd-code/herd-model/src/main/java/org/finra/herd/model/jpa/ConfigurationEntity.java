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
 * This is a configuration that has a key and value. It can be used to hold configuration information for a specific environment.
 */
@Table(name = ConfigurationEntity.TABLE_NAME)
@Entity
public class ConfigurationEntity
{
    /**
     * The table name.
     */
    public static final String TABLE_NAME = "cnfgn";

    /**
     * The key column.
     */
    public static final String COLUMN_KEY = "cnfgn_key_nm";

    /**
     * The value column as a character array.
     */
    public static final String COLUMN_VALUE = "cnfgn_value_ds";

    /**
     * The value column as a CLOB. This currently isn't being used within the configuration entity, but is being used for a Log4J configuration override.
     */
    public static final String COLUMN_VALUE_CLOB = "cnfgn_value_cl";

    @Id
    @Column(name = COLUMN_KEY, length = 100, nullable = false)
    private String key;

    @Column(name = COLUMN_VALUE, length = 512, nullable = false)
    private String value;

    @Column(name = COLUMN_VALUE_CLOB)
    private String valueClob;

    public String getKey()
    {
        return key;
    }

    public void setKey(String key)
    {
        this.key = key;
    }

    public String getValue()
    {
        return value;
    }

    public void setValue(String value)
    {
        this.value = value;
    }

    public String getValueClob()
    {
        return valueClob;
    }

    public void setValueClob(String valueClob)
    {
        this.valueClob = valueClob;
    }
}
