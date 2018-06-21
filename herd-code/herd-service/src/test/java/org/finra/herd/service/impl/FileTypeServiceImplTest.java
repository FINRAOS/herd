package org.finra.herd.service.impl;

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
import org.finra.herd.service.AbstractServiceTest;
import org.finra.herd.service.FileTypeService;
import org.finra.herd.service.helper.AlternateKeyHelper;
import org.finra.herd.service.helper.FileTypeDaoHelper;

/**
 * This class tests functionality within the file type service implementation.
 */
public class FileTypeServiceImplTest extends AbstractServiceTest
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

    private static final List<FileTypeKey> ALL_FILE_TYPE_KEYS = Arrays.asList(
        new FileTypeKey()
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
        }}
    );

    @InjectMocks
    private FileTypeService fileTypeMockService = new FileTypeServiceImpl();

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
        validateCreateFileType(FILE_TYPE_CREATE_REQUEST, FORMAT_FILE_TYPE_CODE);
    }

    @Test
    public void testCreateFileTypeWithExtraSpacesInName()
    {
        validateCreateFileType(FILE_TYPE_CREATE_REQUEST_WITH_EXTRA_SPACES_IN_NAME, FILE_TYPE_CODE_WITH_EXTRA_SPACES);
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
        validateGetFileTypeByKey(FILE_TYPE_KEY);
    }

    @Test
    public void testGetFileTypeNullKey()
    {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("A file type key must be specified");

        fileTypeMockService.getFileType(null);
    }

    @Test
    public void testGetFileTypeWithExtraSpacesInName()
    {
        validateGetFileTypeByKey(FILE_TYPE_KEY_WITH_EXTRA_SPACES_IN_NAME);
    }

    @Test
    public void testDeleteFileType()
    {
        validateDeleteFileTypeByKey(FILE_TYPE_KEY, FORMAT_FILE_TYPE_CODE);
    }

    @Test
    public void testDeleteFileTypeWithExtraSpacesInName()
    {
        validateDeleteFileTypeByKey(FILE_TYPE_KEY_WITH_EXTRA_SPACES_IN_NAME, FILE_TYPE_CODE_WITH_EXTRA_SPACES);
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
        List<FileTypeKey> fileTypeKeyList = fileTypeKeys.getFileTypeKeys();
        assertEquals(ALL_FILE_TYPE_KEYS.size(), fileTypeKeyList.size());

        // verify the order is reserved
        assertEquals(FORMAT_FILE_TYPE_CODE, fileTypeKeyList.get(0).getFileTypeCode());
        assertEquals(FORMAT_FILE_TYPE_CODE_2, fileTypeKeyList.get(1).getFileTypeCode());
        assertEquals(FORMAT_FILE_TYPE_CODE_3, fileTypeKeyList.get(2).getFileTypeCode());

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

    private void validateCreateFileType(FileTypeCreateRequest fileTypeCreateRequest, String fileTypeCode)
    {
        when(fileTypeMockDao.getFileTypeByCode(FORMAT_FILE_TYPE_CODE)).thenReturn(null);
        when(alternateKeyHelper.validateStringParameter(anyString(), anyString())).thenReturn(FORMAT_FILE_TYPE_CODE);
        when(fileTypeMockDao.saveAndRefresh(any(FileTypeEntity.class))).thenReturn(FILE_TYPE_ENTITY);

        FileType fileType = fileTypeMockService.createFileType(fileTypeCreateRequest);
        assertEquals(FORMAT_FILE_TYPE_CODE, fileType.getFileTypeCode());

        verify(alternateKeyHelper).validateStringParameter("file type code", fileTypeCode);
        verify(fileTypeMockDao).getFileTypeByCode(FORMAT_FILE_TYPE_CODE);
        verify(fileTypeMockDao).saveAndRefresh(any(FileTypeEntity.class));

        verifyNoMoreInteractionsHelper();
    }

    private void validateGetFileTypeByKey(FileTypeKey fileTypeKey)
    {
        when(fileTypeDaoHelper.getFileTypeEntity(FORMAT_FILE_TYPE_CODE)).thenReturn(FILE_TYPE_ENTITY);
        when(alternateKeyHelper.validateStringParameter(anyString(), anyString())).thenReturn(FORMAT_FILE_TYPE_CODE);

        FileType fileType = fileTypeMockService.getFileType(fileTypeKey);
        assertEquals(FORMAT_FILE_TYPE_CODE, fileType.getFileTypeCode());
        verify(alternateKeyHelper).validateStringParameter("file type code", fileTypeKey.getFileTypeCode());
        verify(fileTypeDaoHelper).getFileTypeEntity(FORMAT_FILE_TYPE_CODE);

        verifyNoMoreInteractionsHelper();
    }

    private void validateDeleteFileTypeByKey(FileTypeKey fileTypeKey, String fileTypeCode)
    {
        when(fileTypeDaoHelper.getFileTypeEntity(FORMAT_FILE_TYPE_CODE)).thenReturn(FILE_TYPE_ENTITY);
        when(alternateKeyHelper.validateStringParameter(anyString(), anyString())).thenReturn(FORMAT_FILE_TYPE_CODE);

        FileType fileType = fileTypeMockService.deleteFileType(fileTypeKey);
        assertEquals(FORMAT_FILE_TYPE_CODE, fileType.getFileTypeCode());
        verify(alternateKeyHelper).validateStringParameter("file type code", fileTypeCode);
        verify(fileTypeDaoHelper).getFileTypeEntity(FORMAT_FILE_TYPE_CODE);
        verify(fileTypeMockDao).delete(FILE_TYPE_ENTITY);

        verifyNoMoreInteractionsHelper();
    }

    /**
     * Checks if any of the mocks has any interaction.
     */
    private void verifyNoMoreInteractionsHelper()
    {
        verifyNoMoreInteractions(fileTypeDaoHelper, alternateKeyHelper, fileTypeMockDao);
    }
}
