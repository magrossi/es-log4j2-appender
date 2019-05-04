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
import org.apache.logging.log4j.core.appender.AppenderLoggingException;
import org.apache.logging.log4j.core.impl.Log4jLogEvent;
import org.apache.logging.log4j.core.layout.JsonLayout;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.elasticsearch.client.RestClientBuilder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.stubbing.Answer;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.net.InetAddress;
import java.security.InvalidParameterException;
import java.util.TimerTask;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

@RunWith(PowerMockRunner.class)
public class ElasticSearchRestAppenderTest {

	private static final Log4jLogEvent SOME_LOG_EVENT = Log4jLogEvent.newBuilder().setLevel(Level.ERROR).build();
	private static final String SOME_NAME = "someAppenderName";

	@Rule
	public ExpectedException expectedException = ExpectedException.none();

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

	@Test
	public void throwsInvalidParameterExceptionIfLayourIsNull() {
		expectedException.expect(InvalidParameterException.class);
		expectedException.expectMessage("Layout not provided");

		new ElasticSearchRestAppender(SOME_NAME, null, null, true, 0, 0, null, "index", "type", null);
	}

	@Test
	public void throwsInvalidParameterExceptionIfLayourIsNotJson() {
		expectedException.expect(InvalidParameterException.class);
		expectedException.expectMessage("Layout must produce an \"application/json\" content type. Instead it produces \"text/plain\"");

		new ElasticSearchRestAppender(SOME_NAME, null, PatternLayout.createDefaultLayout(), true, 0, 0, null, "index", "type", null);
	}

	@Test
	public void ignoresExceptionsWhileBufferingLogsIfIgnoreExceptionIsTrue() {
		ElasticSearchRestAppender appender = new ElasticSearchRestAppender(SOME_NAME, null, JsonLayout.createDefaultLayout(), true, 0, 0, null, "index", "type", null);

		appender.append(SOME_LOG_EVENT);
	}

	@Test
	public void ignoresExceptionsWhileSendingToEsIfIgnoresExceptionIsTrue() throws IOException {
		doThrow(new RuntimeException("someErrorMessage")).when(mockBulkSender).send(any());
		ElasticSearchRestAppender appender = ((ElasticSearchRestAppender.Builder)baseBuilder().withIgnoreExceptions(true)).build();

		appender.append(SOME_LOG_EVENT);
	}

	@Test
	public void throwsExceptionsWhileSendingToEsIfIgnoresExceptionIsFalse() throws IOException {
		doThrow(new RuntimeException("someErrorMessage")).when(mockBulkSender).send(any());
		ElasticSearchRestAppender appender = baseBuilder().build();

		expectedException.expect(AppenderLoggingException.class);
		expectedException.expectMessage("someErrorMessage");

		appender.append(SOME_LOG_EVENT);
	}

	@Test
	public void throwsAppenderLoggingExceptionWhileBufferingLogsIfIgnoreExceptionIsFalse() {
		expectedException.expect(AppenderLoggingException.class);

		ElasticSearchRestAppender appender = new ElasticSearchRestAppender(SOME_NAME, null, JsonLayout.createDefaultLayout(), false, 0, 0, null, "index", "type", null);

		appender.append(SOME_LOG_EVENT);
	}

	@Test
	public void triesToSendBufferedMessagesBeforeAppenderIsDestroyed() throws IOException {
		ElasticSearchRestAppender appender = ElasticSearchRestAppender.newBuilder().withName(SOME_NAME).withBulkSender(mockBulkSender).build();

		appender.append(SOME_LOG_EVENT);

		appender.stop();
		verify(mockBulkSender).send(anyString());
	}

	@Test
	public void returnsNullWhenNoNameIsProviderFortheBuilder() {
		ElasticSearchRestAppender appender = ElasticSearchRestAppender.newBuilder().build();

		assertThat(appender).isNull();
	}

	@Test
	public void whenDelayExpiresItSendsBufferedLogs() throws IOException {
		ElasticSearchRestAppender appender = baseBuilder().withMaxBulkSize(2).build();
		appender.append(SOME_LOG_EVENT); // buffers a log message
		TimerTask timerTask = appender.timerTask();

		timerTask.run();

		verify(mockBulkSender).send(anyString());
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
