package org.finra.herd.dao.helper;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.dao.JestClientFactory;

/**
 * JestClientHelperTest
 */
public class JestClientHelperTest
{
    private List<Object> createdMocks;

    @InjectMocks
    private JestClientHelper jestClientHelper;

    @Mock
    private JestClientFactory jestClientFactory;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
        createdMocks = new LinkedList<>();
        createdMocks.add(jestClientFactory);
    }

    @Test
    public void testSearchExecute() throws Exception
    {
        // Mock
        Search search = mock(Search.class);
        SearchResult searchResult = mock(SearchResult.class);
        JestClient jestClient = mock(JestClient.class);
        when(jestClientFactory.getJestClient()).thenReturn(jestClient);
        when(jestClient.execute(search)).thenReturn(searchResult);

        // Test
        SearchResult result = jestClientHelper.execute(search);

        // Validate
        assertThat(result, is(not(nullValue())));

        // Verify
        verify(jestClientFactory).getJestClient();
        verify(jestClient).execute(search);
        verifyNoMoreInteractions(createdMocks.toArray());
    }

    @Test
    public void testSearchExecuteWithException() throws Exception
    {
        // Mock
        Search search = mock(Search.class);
        JestClient jestClient = mock(JestClient.class);
        when(jestClientFactory.getJestClient()).thenReturn(jestClient);
        when(jestClient.execute(search)).thenThrow(new IOException());

        try
        {
            // Test
            jestClientHelper.execute(search);
        }
        catch (RuntimeException runtimeException)
        {
            // Validate
            assertThat(runtimeException, is(instanceOf(RuntimeException.class)));
        }

        // Verify
        verify(jestClientFactory).getJestClient();
        verify(jestClient).execute(search);
        verifyNoMoreInteractions(createdMocks.toArray());
    }

}
