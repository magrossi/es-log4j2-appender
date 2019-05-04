/*  
 *  Copyright 2017 Marcelo Grossi
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0*
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.github.magrossi.log4j2.elasticsearch;

import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.impl.Log4jLogEvent;
import org.apache.logging.log4j.core.layout.JsonLayout;
import org.elasticsearch.client.RestClientBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.stubbing.Answer;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.net.InetAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

@RunWith(PowerMockRunner.class)
public class ElasticSearchRestAppenderTest {

	private static final Log4jLogEvent SOME_LOG_EVENT = Log4jLogEvent.newBuilder().setLevel(Level.ERROR).build();
	private static final String SOME_NAME = "someAppenderName";

	@Mock
	private HttpAsyncClientBuilder mockHttpAsyncClientBuilder;

	@Mock
	private BulkSender mockBulkSender;

	@Test
	public void sendsLogsImmediately() throws IOException {
		ElasticSearchRestAppender appender = baseBuilder().build();

		appender.append(SOME_LOG_EVENT);

		verify(mockBulkSender).send(anyString());
	}

	@Test
	public void waitsBeforeSendingLogs() throws IOException, InterruptedException, ExecutionException {
		long waitInMillis = 500L;

		ElasticSearchRestAppender appender = baseBuilder().withMaxDelayTime(waitInMillis).build();

		assertLogSentAfterMillis(appender, waitInMillis);
	}

	@Test
	public void sendsLogsAfterNumberOfCalls() throws IOException {
		int numberOfCalls = 3;

		ElasticSearchRestAppender appender = baseBuilder().withMaxBulkSize(numberOfCalls).build();

		assertLogSentAfterNumberOfCalls(appender, numberOfCalls);
	}

	@Test
	public void sendsLogsAfterATimeOrAfterNumberOfCalls() throws IOException, InterruptedException, ExecutionException {
		long waitInMillis = 500L;
		int numberOfCalls = 3;

		ElasticSearchRestAppender appender = baseBuilder().withMaxBulkSize(numberOfCalls).withMaxDelayTime(waitInMillis).build();

		assertLogSentAfterMillis(appender, waitInMillis);
		assertLogSentAfterNumberOfCalls(appender, numberOfCalls);
	}

	@Test
	public void defaultRestClientSetsCredentialsWhenUserIsSupplied() {
		RestClientBuilder.HttpClientConfigCallback httpClientConfigCallback = ElasticSearchRestAppender.Builder.httpClientConfigCallback("someUser", "somePassword");

		httpClientConfigCallback.customizeHttpClient(mockHttpAsyncClientBuilder);

		verify(mockHttpAsyncClientBuilder).setDefaultCredentialsProvider(isA(CredentialsProvider.class));
	}

	@Test
	public void defaultRestClientDoesNotSetCredentialsWhenUserIsNotSupplied() {
		RestClientBuilder.HttpClientConfigCallback httpClientConfigCallback = ElasticSearchRestAppender.Builder.httpClientConfigCallback(null, null);

		httpClientConfigCallback.customizeHttpClient(mockHttpAsyncClientBuilder);

		verifyZeroInteractions(mockHttpAsyncClientBuilder);
	}

	private ElasticSearchRestAppender.Builder baseBuilder() {
		return ElasticSearchRestAppender.newBuilder()
				.withName(SOME_NAME)
				.withBulkSender(mockBulkSender)
				.withCredentials("someUser", "somePassword")
				.withDateFormat("yyyyMMdd")
				.withHosts(HttpAddress.newBuilder().withHost(InetAddress.getLoopbackAddress()).withPort(999).withScheme("someScheme").build())
				.withIndex("someIndex")
				.withType("someType")
				.withIgnoreExceptions(false)
				.withLayout(JsonLayout.createDefaultLayout())
				.withFilter(null)
			    .withMaxBulkSize(0)
				.withMaxDelayTime(0L);
	}

	private void assertLogSentAfterNumberOfCalls(ElasticSearchRestAppender appender, int numberOfCalls) throws IOException {
		reset(mockBulkSender);

		for (int i = 1; i < numberOfCalls; i++) {
			appender.append(SOME_LOG_EVENT);
		}

		verifyZeroInteractions(mockBulkSender);

		appender.append(SOME_LOG_EVENT);

		verify(mockBulkSender).send(anyString());
	}

	private void assertLogSentAfterMillis(ElasticSearchRestAppender appender, Long millis) throws IOException, ExecutionException, InterruptedException {
		reset(mockBulkSender);

		CompletableFuture<Long> future = new CompletableFuture<>();
		long start = System.nanoTime();
		doAnswer((Answer<Void>) invocation -> {
			future.complete(System.nanoTime() - start);
			return null;
		}).when(mockBulkSender).send(anyString());

		appender.append(SOME_LOG_EVENT);

		Long timeInMillisWhenSendWasCalled = future.get();
		assertThat(timeInMillisWhenSendWasCalled).isGreaterThanOrEqualTo(millis * 1000);
	}
}
