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
package org.finra.herd.dao;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.persistence.PersistenceException;
import javax.sql.DataSource;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.jdbc.core.JdbcTemplate;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.helper.HerdDaoSecurityHelper;
import org.finra.herd.dao.impl.StorageFileDaoImpl;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StorageFileEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;

public class StorageFileMockDaoTest extends AbstractDaoTest
{
    @Mock
    private ConfigurationHelper configurationHelper;

    @Mock
    private Connection connection;

    @Mock
    private DataSource dataSource;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Mock
    private HerdDaoSecurityHelper herdDaoSecurityHelper;

    @Mock
    private JdbcTemplate jdbcTemplate;

    @Mock
    private PreparedStatement preparedStatement;

    @InjectMocks
    private StorageFileDaoImpl storageFileDao;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testSaveStorageFilesBatchUpdateException() throws Exception
    {
        final String INSERT_INTO_STORAGE_FILE_TABLE_SQL = "INSERT INTO strge_file " +
            "(strge_file_id, fully_qlfd_file_nm, file_size_in_bytes_nb, row_ct, strge_unit_id, creat_ts, creat_user_id) " +
            "VALUES (nextval('strge_file_seq'), ?, ?, ?, ?, current_timestamp, '" + USER_CREDENTIAL_NAME + "')";

        // Create a storage unit entity
        StorageUnitEntity storageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(StorageEntity.MANAGED_STORAGE, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION,
                PARTITION_VALUE, SUBPARTITION_VALUES, INITIAL_DATA_VERSION, true, BDATA_STATUS, StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);

        // Mock the external calls.
        when(herdDaoSecurityHelper.getCurrentUsername()).thenReturn(USER_CREDENTIAL_NAME);
        when(jdbcTemplate.getDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
        when(configurationHelper.getProperty(ConfigurationValue.JDBC_BATCH_SIZE, Integer.class)).thenReturn(100);
        when(connection.prepareStatement(INSERT_INTO_STORAGE_FILE_TABLE_SQL)).thenReturn(preparedStatement);
        when(preparedStatement.executeBatch()).thenThrow(new BatchUpdateException());

        List<StorageFileEntity> storageFileEntities = new ArrayList<>();
        for (String file : LOCAL_FILES)
        {
            StorageFileEntity storageFileEntity = new StorageFileEntity();
            storageFileEntity.setStorageUnit(storageUnitEntity);
            storageFileEntity.setPath(file);
            storageFileEntity.setFileSizeBytes(FILE_SIZE_1_KB);
            storageFileEntity.setRowCount(ROW_COUNT_1000);
            storageFileEntities.add(storageFileEntity);
        }

        // Specify the expected exception.
        expectedException.expect(PersistenceException.class);

        // Call method under test
        storageFileDao.saveStorageFiles(storageFileEntities);

        // Verify the external calls.
        verify(herdDaoSecurityHelper).getCurrentUsername();
        verify(jdbcTemplate).getDataSource();
        verify(dataSource).getConnection();
        verify(configurationHelper).getProperty(ConfigurationValue.JDBC_BATCH_SIZE, Integer.class);
        verify(connection).prepareStatement(INSERT_INTO_STORAGE_FILE_TABLE_SQL);
        verifyNoMoreInteractions(configurationHelper, connection, dataSource, jdbcTemplate, herdDaoSecurityHelper, preparedStatement);
    }

    @Test
    public void testSaveStorageFilesSqlException() throws Exception
    {
        final String INSERT_INTO_STORAGE_FILE_TABLE_SQL = "INSERT INTO strge_file " +
            "(strge_file_id, fully_qlfd_file_nm, file_size_in_bytes_nb, row_ct, strge_unit_id, creat_ts, creat_user_id) " +
            "VALUES (nextval('strge_file_seq'), ?, ?, ?, ?, current_timestamp, '" + USER_CREDENTIAL_NAME + "')";

        // Create a storage unit entity
        StorageUnitEntity storageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(StorageEntity.MANAGED_STORAGE, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION,
                PARTITION_VALUE, SUBPARTITION_VALUES, INITIAL_DATA_VERSION, true, BDATA_STATUS, StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);

        // Mock the external calls.
        when(herdDaoSecurityHelper.getCurrentUsername()).thenReturn(USER_CREDENTIAL_NAME);
        when(jdbcTemplate.getDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
        when(configurationHelper.getProperty(ConfigurationValue.JDBC_BATCH_SIZE, Integer.class)).thenReturn(100);
        when(connection.prepareStatement(INSERT_INTO_STORAGE_FILE_TABLE_SQL)).thenReturn(preparedStatement);
        when(preparedStatement.executeBatch()).thenThrow(new SQLException());

        List<StorageFileEntity> storageFileEntities = new ArrayList<>();
        for (String file : LOCAL_FILES)
        {
            StorageFileEntity storageFileEntity = new StorageFileEntity();
            storageFileEntity.setStorageUnit(storageUnitEntity);
            storageFileEntity.setPath(file);
            storageFileEntity.setFileSizeBytes(FILE_SIZE_1_KB);
            storageFileEntity.setRowCount(ROW_COUNT_1000);
            storageFileEntities.add(storageFileEntity);
        }

        // Specify the expected exception.
        expectedException.expect(PersistenceException.class);

        // Call method under test
        storageFileDao.saveStorageFiles(storageFileEntities);

        // Verify the external calls.
        verify(herdDaoSecurityHelper).getCurrentUsername();
        verify(jdbcTemplate).getDataSource();
        verify(dataSource).getConnection();
        verify(configurationHelper).getProperty(ConfigurationValue.JDBC_BATCH_SIZE, Integer.class);
        verify(connection).prepareStatement(INSERT_INTO_STORAGE_FILE_TABLE_SQL);
        verifyNoMoreInteractions(configurationHelper, connection, dataSource, jdbcTemplate, herdDaoSecurityHelper, preparedStatement);
    }
}
