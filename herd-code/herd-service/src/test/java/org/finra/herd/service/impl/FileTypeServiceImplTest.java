package org.finra.herd.service.impl;

import static org.finra.herd.dao.AbstractDaoTest.CREATED_BY;
import static org.finra.herd.dao.AbstractDaoTest.CREATED_ON;
import static org.finra.herd.dao.AbstractDaoTest.FORMAT_FILE_TYPE_CODE;
import static org.finra.herd.dao.AbstractDaoTest.FORMAT_FILE_TYPE_CODE_2;
import static org.finra.herd.dao.AbstractDaoTest.FORMAT_FILE_TYPE_CODE_3;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.dao.FileTypeDao;
import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.api.xml.FileType;
import org.finra.herd.model.api.xml.FileTypeCreateRequest;
import org.finra.herd.model.api.xml.FileTypeKey;
import org.finra.herd.model.api.xml.FileTypeKeys;
import org.finra.herd.model.jpa.FileTypeEntity;
import org.finra.herd.service.helper.AlternateKeyHelper;
import org.finra.herd.service.helper.FileTypeDaoHelper;

/**
 * This class tests functionality within the file type service implementation.
 */
public class FileTypeServiceImplTest
{
    private static final String FILE_TYPE_CODE_WITH_EXTRA_SPACES = FORMAT_FILE_TYPE_CODE + "    ";

    private static final FileTypeCreateRequest FILE_TYPE_CREATE_REQUEST = new FileTypeCreateRequest()
    {{
        setFileTypeCode(FORMAT_FILE_TYPE_CODE);
    }};

    private static final FileTypeCreateRequest FILE_TYPE_CREATE_REQUEST_WITH_EXTRA_SPACES_IN_NAME = new FileTypeCreateRequest()
    {{
        setFileTypeCode(FILE_TYPE_CODE_WITH_EXTRA_SPACES);
    }};

    private static final FileTypeKey FILE_TYPE_KEY = new FileTypeKey()
    {{
        setFileTypeCode(FORMAT_FILE_TYPE_CODE);
    }};

    private static final FileTypeKey FILE_TYPE_KEY_WITH_EXTRA_SPACES_IN_NAME = new FileTypeKey()
    {{
        setFileTypeCode(FILE_TYPE_CODE_WITH_EXTRA_SPACES);
    }};

    private static final FileTypeEntity FILE_TYPE_ENTITY = new FileTypeEntity()
    {{
        setCode(FORMAT_FILE_TYPE_CODE);
        setCreatedBy(CREATED_BY);
        setUpdatedBy(CREATED_BY);
        setCreatedOn(new Timestamp(CREATED_ON.getMillisecond()));
    }};

    private static final List<FileTypeKey> ALL_FILE_TYPE_KEYS = Arrays.asList(new FileTypeKey()
        {{
            setFileTypeCode(FORMAT_FILE_TYPE_CODE);
        }},

        new FileTypeKey()
        {{
            setFileTypeCode(FORMAT_FILE_TYPE_CODE_2);
        }},

        new FileTypeKey()
        {{
            setFileTypeCode(FORMAT_FILE_TYPE_CODE_3);
        }});

    @InjectMocks
    private FileTypeServiceImpl fileTypeMockService;

    @Mock
    private AlternateKeyHelper alternateKeyHelper;

    @Mock
    private FileTypeDao fileTypeMockDao;

    @Mock
    private FileTypeDaoHelper fileTypeDaoHelper;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCreateFileType()
    {
        when(fileTypeMockDao.getFileTypeByCode(FORMAT_FILE_TYPE_CODE)).thenReturn(null);
        when(alternateKeyHelper.validateStringParameter(anyString(), anyString())).thenReturn(FORMAT_FILE_TYPE_CODE);
        when(fileTypeMockDao.saveAndRefresh(any(FileTypeEntity.class))).thenReturn(FILE_TYPE_ENTITY);

        FileType fileType = fileTypeMockService.createFileType(FILE_TYPE_CREATE_REQUEST);
        assertEquals(FORMAT_FILE_TYPE_CODE, fileType.getFileTypeCode());

        verify(alternateKeyHelper).validateStringParameter("file type code", FORMAT_FILE_TYPE_CODE);
        verify(fileTypeMockDao).getFileTypeByCode(FORMAT_FILE_TYPE_CODE);
        verify(fileTypeMockDao).saveAndRefresh(any(FileTypeEntity.class));

        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testCreateFileTypeAlreadyExists()
    {
        expectedException.expect(AlreadyExistsException.class);
        expectedException.expectMessage(String.format("Unable to create file type \"%s\" because it already exists.", FORMAT_FILE_TYPE_CODE));

        when(fileTypeMockDao.getFileTypeByCode(FORMAT_FILE_TYPE_CODE)).thenReturn(FILE_TYPE_ENTITY);
        when(alternateKeyHelper.validateStringParameter(anyString(), anyString())).thenReturn(FORMAT_FILE_TYPE_CODE);
        fileTypeMockService.createFileType(FILE_TYPE_CREATE_REQUEST);
    }

    @Test
    public void testGetFileType()
    {
        when(fileTypeDaoHelper.getFileTypeEntity(FORMAT_FILE_TYPE_CODE)).thenReturn(FILE_TYPE_ENTITY);
        when(alternateKeyHelper.validateStringParameter(anyString(), anyString())).thenReturn(FORMAT_FILE_TYPE_CODE);

        FileType fileType = fileTypeMockService.getFileType(FILE_TYPE_KEY);
        assertEquals(FORMAT_FILE_TYPE_CODE, fileType.getFileTypeCode());
        verify(alternateKeyHelper).validateStringParameter("file type code", FILE_TYPE_KEY.getFileTypeCode());
        verify(fileTypeDaoHelper).getFileTypeEntity(FORMAT_FILE_TYPE_CODE);

        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testGetFileTypeNullKey()
    {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("A file type key must be specified");

        fileTypeMockService.getFileType(null);
    }

    @Test
    public void testDeleteFileType()
    {
        when(fileTypeDaoHelper.getFileTypeEntity(FORMAT_FILE_TYPE_CODE)).thenReturn(FILE_TYPE_ENTITY);
        when(alternateKeyHelper.validateStringParameter(anyString(), anyString())).thenReturn(FORMAT_FILE_TYPE_CODE);

        FileType fileType = fileTypeMockService.deleteFileType(FILE_TYPE_KEY);
        assertEquals(FORMAT_FILE_TYPE_CODE, fileType.getFileTypeCode());
        verify(alternateKeyHelper).validateStringParameter("file type code", FORMAT_FILE_TYPE_CODE);
        verify(fileTypeDaoHelper).getFileTypeEntity(FORMAT_FILE_TYPE_CODE);
        verify(fileTypeMockDao).delete(FILE_TYPE_ENTITY);

        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testDeleteFileTypeNullKey()
    {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("A file type key must be specified");

        fileTypeMockService.deleteFileType(null);
    }

    @Test
    public void testGetFileTypes()
    {
        when(fileTypeMockDao.getFileTypes()).thenReturn(ALL_FILE_TYPE_KEYS);

        FileTypeKeys fileTypeKeys = fileTypeMockService.getFileTypes();

        assertNotNull(fileTypeKeys);
        assertEquals(ALL_FILE_TYPE_KEYS, fileTypeKeys.getFileTypeKeys());

        verify(fileTypeMockDao).getFileTypes();

        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testGetFileTypesEmptyList()
    {
        when(fileTypeMockDao.getFileTypes()).thenReturn(Collections.emptyList());
        FileTypeKeys fileTypeKeys = fileTypeMockService.getFileTypes();

        assertNotNull(fileTypeKeys);
        assertEquals(0, fileTypeKeys.getFileTypeKeys().size());

        verify(fileTypeMockDao).getFileTypes();

        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testValidateFileTypeCreateRequestExtraSpaces() {
        when(alternateKeyHelper.validateStringParameter(anyString(), anyString())).thenReturn(FORMAT_FILE_TYPE_CODE);

        assertEquals(FILE_TYPE_CODE_WITH_EXTRA_SPACES, FILE_TYPE_CREATE_REQUEST_WITH_EXTRA_SPACES_IN_NAME.getFileTypeCode());
        fileTypeMockService.validateFileTypeCreateRequest(FILE_TYPE_CREATE_REQUEST_WITH_EXTRA_SPACES_IN_NAME);
        // White space should be trimmed now
        assertEquals(FORMAT_FILE_TYPE_CODE, FILE_TYPE_CREATE_REQUEST_WITH_EXTRA_SPACES_IN_NAME.getFileTypeCode());
    }

    @Test
    public void testValidateAndTrimFileTypeKeyExtraSpaces() {
        when(alternateKeyHelper.validateStringParameter(anyString(), anyString())).thenReturn(FORMAT_FILE_TYPE_CODE);

        assertEquals(FILE_TYPE_CODE_WITH_EXTRA_SPACES, FILE_TYPE_KEY_WITH_EXTRA_SPACES_IN_NAME.getFileTypeCode());
        fileTypeMockService.validateAndTrimFileTypeKey(FILE_TYPE_KEY_WITH_EXTRA_SPACES_IN_NAME);
        // White space should be trimmed now
        assertEquals(FORMAT_FILE_TYPE_CODE, FILE_TYPE_KEY_WITH_EXTRA_SPACES_IN_NAME.getFileTypeCode());
    }

    /**
     * Checks if any of the mocks has any interaction.
     */
    private void verifyNoMoreInteractionsHelper()
    {
        verifyNoMoreInteractions(fileTypeDaoHelper, alternateKeyHelper, fileTypeMockDao);
    }
}
