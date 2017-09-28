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

import static org.junit.Assert.*;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.MarkerManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.impl.Log4jLogEvent;
import org.apache.logging.log4j.core.layout.JsonLayout;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.magrossi.log4j2.elasticsearch.ElasticSearchRestAppender;

/**
 * Test Class for ElasticSearchRestAppender
 */
public class ElasticSearchRestAppenderTest {
	
	private static String CONFIG_LOCATION = "src/test/resources/test.properties";

	/**
	 * Helper Elastic client for finding logs
	 */
	public static class ElasticClient {

		private static Map<String,String> parms = new HashMap<>();
		private static Header[] headers = new Header[] { new BasicHeader("Content-Type", "application/json") }; 
		
		private final RestClient client;
		private final String index;
		private final String type;

		public ElasticClient(Logger logger) {
			Map<String,Appender> appenders = logger.getAppenders();
			ElasticSearchRestAppender appender = (ElasticSearchRestAppender)appenders.get(appenders.keySet().iterator().next());
			client = RestClient.builder(new HttpHost("localhost", 9200)).build();
			index = appender.getIndex();
			type = appender.getType();
		}

		/**
		 * Queries the Elastic cluster and returns the document hits
		 *
		 * @param query the query
		 * @return the tree node pointing to the raw document results
		 * @throws IOException Signals that an I/O exception has occurred.
		 */
		private JsonNode query(String query) throws IOException {
			// Refresh the index first
			client.performRequest("GET", String.format("%s*/_refresh", index), headers);

			// Then query for results
			Response response = client.performRequest("GET", String.format("%s*/%s/_search", index, type), parms, new NStringEntity(query), headers);
			String body = EntityUtils.toString(response.getEntity());
			
			// Convert to TreeNode and position at { hits.hits: [..] }
			ObjectMapper mapper = new ObjectMapper();
			JsonNode result = mapper.readTree(body);
			return result.get("hits").get("hits");
		}

		/**
		 * Finds all the logs with given marker name
		 *
		 * @param marker the marker name
		 * @return the tree node
		 * @throws IOException Signals that an I/O exception has occurred.
		 */
		public JsonNode findAllByMarker(String marker) throws IOException {
			return this.query(String.format("{\"query\": {\"match\": {\"marker.name\": \"%s\"}},\"sort\":[{\"message.keyword\":{\"order\":\"asc\"}}]}", marker));
					
		}
		
		/**
		 * Finds the first log with marker name
		 *
		 * @param marker the marker name
		 * @return the tree node
		 * @throws IOException Signals that an I/O exception has occurred.
		 */
		public JsonNode findFirstByMarker(String marker) throws IOException {
			JsonNode hits = this.query(String.format("{\"query\": {\"match\": {\"marker.name\": \"%s\"}},\"size\": 1,\"sort\":[{\"message.keyword\":{\"order\":\"asc\"}}]}", marker));
			assertNotNull(hits);
			if (hits.isArray() && hits.size() > 0) {
				return hits.get(0).get("_source");
			} else {
				return null;
			}
		}

	}
	
	/**
	 * Gets the test logger and assign the appender to it 
	 *
	 * @param appender the name of the appender
	 * @return the logger
	 */
	private Logger getLogger(String appender) {
		final LoggerContext loggerContext = Configurator.initialize(getUniqueMarker(), CONFIG_LOCATION);
		Logger logger = loggerContext.getLogger(appender);
		logger.getAppenders().clear();
		logger.addAppender(loggerContext.getConfiguration().getAppenders().get(appender));
		return logger;
	}

	private String getUniqueMarker() {
		return UUID.randomUUID().toString();
	}
	
	/**
	 * Asserts that the fieldName field in node have the expected value. 
	 *
	 * @param node the node
	 * @param fieldName the field name
	 * @param expected the expected
	 */
	private static void assertFieldValue(JsonNode node, String fieldName, Object expected) {
		JsonNode field = node.get(fieldName);
		assertNotNull(field);
		assertTrue(field.isValueNode());
		if (field.isNull()) {
			assertNull(expected);
		} else if (field.isTextual()) {
			assertEquals(expected, field.asText());
		} else if (field.isNumber()) {
			assertEquals(expected, field.numberValue());
		} else if (field.isBoolean()) {
			assertEquals(expected, field.asBoolean());
		} else {
			assertEquals(expected.toString(), field.toString());
		}
	}

	/**
	 * 
	 * Builder tests
	 * 
	 */
	
	/**
	 * Tests for Builder for both HttpAddress and ElasticSearchRestAppender.
	 *
	 * @throws Exception the exception
	 */
	@Test
	public void builderTests() throws Exception {
		InetAddress address = InetAddress.getByName("invalid.hostname.com");
		HttpAddress host = HttpAddress.newBuilder()
				.withHost(address)
				.withPort(123)
				.withScheme("https")
				.build();
		
		assertEquals("invalid.hostname.com", host.getHostName());
		assertEquals("https", host.getSchemeName());
		assertEquals(123, host.getPort());
		assertEquals(address, host.getAddress());
		
		JsonLayout layout = JsonLayout.newBuilder()
    			.setCompact(true)
    			.setIncludeStacktrace(true)
    			.setLocationInfo(true)
    			.setProperties(true)
    			.build();
		
		ElasticSearchRestAppender appender = ElasticSearchRestAppender.newBuilder()
				.withName("builder-tests")
				.withIndex("test-index-")
				.withType("test-type")
				.withLayout(layout)
				.withCredentials("test-user", "test-pwd")
				.withDateFormat("dd-MM-yyyy")
				.withHosts(host)
				.withMaxBulkSize(123)
				.withMaxDelayTime(1234L)
				.withIgnoreExceptions(false)
				.build();
		
		assertEquals(123, appender.getMaxBulkSize());
		assertEquals(1234L, appender.getMaxDelayTime());
		Calendar calendar = Calendar.getInstance();
		calendar.set(2013, 11, 05);
		assertEquals("05-12-2013", appender.getDateFormat().format(calendar.getTime()));

		assertEquals(ElasticBulkSender.class, appender.getBulkSender().getClass());
		ElasticBulkSender sender = (ElasticBulkSender)appender.getBulkSender();
		assertEquals(1, sender.getHosts().length);
		assertEquals(host.getHttpHost(), sender.getHosts()[0]);
		assertNotNull(sender.getRestClient());
		assertEquals("test-user", sender.getUser());
		assertEquals("test-pwd", sender.getPassword());
		assertEquals("test-index-", appender.getIndex());
		assertEquals("test-type", appender.getType());
		assertEquals(layout, appender.getLayout());
		
		
	}

	/**
	 * Builder tests with custom sender.
	 */
	@Test
	public void builderTestsWithCustomSender() {
		BulkSender sender = Mockito.mock(BulkSender.class);
		ElasticSearchRestAppender appender = ElasticSearchRestAppender.newBuilder()
				.withName("builder-tests-with-custom-sender")
				.withBulkSender(sender)
				.build();
		assertEquals(sender, appender.getBulkSender());
		assertEquals(200, appender.getMaxBulkSize());
		assertEquals(2000L, appender.getMaxDelayTime());
	}
	
	/**
	 * 
	 * Integration tests that check events reach the Elastic cluster.
	 * The cluster must be running on http://localhost:9200.
	 * 
	 */	
	
	/**
	 * Integration test that asserts events are sent to the Elastic cluster.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	@Test
	public void integrationTest() throws IOException {
		String test = "integration-test";
		Logger logger = getLogger(test);
		ElasticClient client = new ElasticClient(logger);
		String marker = getUniqueMarker();
		logger.error(MarkerManager.getMarker(marker), "Test Message");
		JsonNode doc = client.findFirstByMarker(marker);
		assertNotNull(doc);
		assertFieldValue(doc, "level", "ERROR");
		assertFieldValue(doc, "message", "Test Message");
	}

	/**
	 * Integration test that asserts events are sent to the Elastic cluster in bulk.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	@Test
	public void integrationTestAfter5() throws IOException {
		String test = "integration-test-after-5";
		Logger logger = getLogger(test);
		ElasticClient client = new ElasticClient(logger);
		String marker = getUniqueMarker();

		// Should not send until it reaches 5 events
		for (int i = 1; i < 5; i++) {
			logger.error(MarkerManager.getMarker(marker), i + "-" + test);
		}
		JsonNode doc = client.findFirstByMarker(marker);
		assertNull(doc);
		
		// But after the 5th event it should send all buffered events
		logger.error(MarkerManager.getMarker(marker), "5-" + test);
		JsonNode hits = client.findAllByMarker(marker);
		assertNotNull(hits);
		assertEquals(5, hits.size());
		for (int i = 0; i < 5; i++) {
			assertFieldValue(hits.get(i).get("_source"), "level", "ERROR");
			assertFieldValue(hits.get(i).get("_source"), "message", (i + 1) + "-" + test);
		}
	}
	
	/**
	 * 
	 * Specific tests for the buffering of events	
	 * 
	 */

	/**
	 * Dummy event for buffering tests
	 * 
	 * @return the dummy event
	 */
	private LogEvent getLogEvent() {
		return Log4jLogEvent.newBuilder()
				.setLevel(Level.ERROR)
				.build();
	}
	
	/**
	 * Test that appender sends event immediately upon receiving them.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	@Test
	public void testNoWait() throws IOException {
		BulkSender sender = Mockito.mock(BulkSender.class);
		ElasticSearchRestAppender appender = ElasticSearchRestAppender.newBuilder()
			.withName("test-no-wait")
			.withBulkSender(sender)
			.withMaxBulkSize(0)
			.withMaxDelayTime(0L)
			.build();
		verify(sender, times(0)).send(anyString());
		appender.append(getLogEvent());
		verify(sender, times(1)).send(anyString());
		appender.append(getLogEvent());
		verify(sender, times(2)).send(anyString());
	}
	

	/**
	 * Test that appender buffers events for 3 seconds before sending.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws InterruptedException the interrupted exception
	 * @throws ExecutionException the execution exception
	 */
	@Test
	public void test3sWait() throws IOException, InterruptedException, ExecutionException {
		BulkSender sender = Mockito.mock(BulkSender.class);
		ElasticSearchRestAppender appender = ElasticSearchRestAppender.newBuilder()
			.withName("test-3s-wait")
			.withBulkSender(sender)
			.withMaxBulkSize(0)
			.withMaxDelayTime(3000L)
			.build();
		verify(sender, times(0)).send(anyString());
		appender.append(getLogEvent());
		verify(sender, times(0)).send(anyString());
		CompletableFuture<Long> future = new CompletableFuture<>();
		long start = System.nanoTime();
		doAnswer(new Answer<Void>() {
			@Override
			public Void answer(InvocationOnMock invocation) throws Throwable {
				long elapsed = System.nanoTime() - start;
				future.complete(elapsed); 
				return null;
			}
		}).when(sender).send(anyString());
		assertTrue(future.get() >= 3000000);
		verify(sender, times(1)).send(anyString());
	}
	
	@Test
	public void testAfter5() throws IOException, InterruptedException {
		BulkSender sender = Mockito.mock(BulkSender.class);
		ElasticSearchRestAppender appender = ElasticSearchRestAppender.newBuilder()
			.withName("test-after-5")
			.withBulkSender(sender)
			.withMaxBulkSize(5)
			.withMaxDelayTime(0L)
			.build();
		verify(sender, times(0)).send(anyString());
		appender.append(getLogEvent());
		verify(sender, times(0)).send(anyString());
		appender.append(getLogEvent());
		verify(sender, times(0)).send(anyString());
		appender.append(getLogEvent());
		verify(sender, times(0)).send(anyString());
		appender.append(getLogEvent());
		verify(sender, times(0)).send(anyString());
		appender.append(getLogEvent());
		verify(sender, times(1)).send(anyString());
	}
	
	@Test
	public void test3sWaitOrAfter5() throws IOException, InterruptedException, ExecutionException {
		BulkSender sender = Mockito.mock(BulkSender.class);
		ElasticSearchRestAppender appender = ElasticSearchRestAppender.newBuilder()
			.withName("test-3s-wait-or-after-5")
			.withBulkSender(sender)
			.withMaxBulkSize(5)
			.withMaxDelayTime(3000L)
			.build();
		verify(sender, times(0)).send(anyString());
		appender.append(getLogEvent());
		verify(sender, times(0)).send(anyString());
		appender.append(getLogEvent());
		verify(sender, times(0)).send(anyString());
		appender.append(getLogEvent());
		verify(sender, times(0)).send(anyString());
		appender.append(getLogEvent());
		verify(sender, times(0)).send(anyString());
		appender.append(getLogEvent());
		verify(sender, times(1)).send(anyString());
		appender.append(getLogEvent());
		verify(sender, times(1)).send(anyString());
		CompletableFuture<Long> future = new CompletableFuture<>();
		long start = System.nanoTime();
		doAnswer(new Answer<Void>() {
			@Override
			public Void answer(InvocationOnMock invocation) throws Throwable {
				long elapsed = System.nanoTime() - start;
				future.complete(elapsed); 
				return null;
			}
		}).when(sender).send(anyString());
		assertTrue(future.get() >= 3000000);
		verify(sender, times(2)).send(anyString());
	}

}
