package com.github.magrossi.log4j2.elasticsearch;

import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpResponseException;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ElasticBulkSenderTest {

    private static final String SOME_JSON_BODY = "{}";
    private static final Integer SOME_ERROR_CODE = 500;
    private static final Integer SOME_SUCCESS_CODE = 200;

    @Mock
    private StatusLine mockStatusLine;

    @Mock
    private Response mockResponse;

    @Mock
    private RestClient mockRestClient;

    @InjectMocks
    private ElasticBulkSender elasticBulkSender;

    @Before
    public void setUp() throws Exception {
        when(mockStatusLine.getStatusCode()).thenReturn(SOME_SUCCESS_CODE);
        when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
        when(mockRestClient.performRequest(anyString(), anyString(), anyMapOf(String.class, String.class), isA(HttpEntity.class))).thenReturn(mockResponse);
    }

    @Test
    public void delegatesSendToRestClient() throws IOException {
        NStringEntity bodyEntity = new NStringEntity(SOME_JSON_BODY, ContentType.APPLICATION_JSON);

        elasticBulkSender.send(SOME_JSON_BODY);

        ArgumentCaptor<NStringEntity> argCaptor = ArgumentCaptor.forClass(NStringEntity.class);
        verify(mockRestClient).performRequest(eq("POST"), eq("_bulk"), eq(Collections.emptyMap()), argCaptor.capture());
        NStringEntity actualBodyEntity = argCaptor.getValue();
        assertThat(actualBodyEntity).isEqualToComparingFieldByFieldRecursively(bodyEntity);
    }

    @Test(expected = HttpResponseException.class)
    public void throwsHttpResponseExceptionIfResponseNot1xxOr2xx() throws IOException {
        when(mockStatusLine.getStatusCode()).thenReturn(SOME_ERROR_CODE);

        elasticBulkSender.send(SOME_JSON_BODY);
    }
}