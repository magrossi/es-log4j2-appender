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
package org.magrossi.log4j2.elasticsearch;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.MarkerManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configurator;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.junit.Test;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

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
			client = RestClient.builder(appender.getHosts()).build();
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

	@Test
	public void testNoWait() throws IOException {
		String test = "test-no-wait";
		Logger logger = getLogger(test);
		ElasticClient client = new ElasticClient(logger);		
		String marker = getUniqueMarker();
		
		// Should send straight away
		logger.error(MarkerManager.getMarker(marker), "1-" + test);
		JsonNode doc = client.findFirstByMarker(marker);
		assertNotNull(doc);
		assertFieldValue(doc, "level", "ERROR");
		assertFieldValue(doc, "message", "1-" + test);
	}
	
	@Test
	public void test3sWait() throws IOException, InterruptedException {
		String test = "test-3s-wait";
		Logger logger = getLogger(test);
		ElasticClient client = new ElasticClient(logger);
		String marker = getUniqueMarker();
		
		// Should not send it straight away
		logger.error(MarkerManager.getMarker(marker), "1-" + test);
		logger.error(MarkerManager.getMarker(marker), "2-" + test);
		JsonNode doc = client.findFirstByMarker(marker);
		assertNull(doc);
		
		// But after 3 seconds has passed we shall find two
		Thread.sleep(3000);
		JsonNode hits = client.findAllByMarker(marker);
		assertNotNull(hits);
		assertEquals(2, hits.size());
		assertFieldValue(hits.get(0).get("_source"), "message", "1-" + test);
		assertFieldValue(hits.get(1).get("_source"), "message", "2-" + test);
	}
	
	@Test
	public void testAfter5() throws IOException, InterruptedException {
		String test = "test-after-5";
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
			assertFieldValue(hits.get(i).get("_source"), "message", (i + 1) + "-" + test);
		}
	}
	
	@Test
	public void test3sWaitOrAfter5() throws IOException, InterruptedException {
		String test = "test-3s-wait-or-after-5";
		Logger logger = getLogger(test);
		ElasticClient client = new ElasticClient(logger);		
		String marker = getUniqueMarker();
		
		// Should not send it straight away
		logger.error(MarkerManager.getMarker(marker), "0-" + test);
		JsonNode doc = client.findFirstByMarker(marker);
		assertNull(doc);

		// But after 3 seconds we shall find it
		Thread.sleep(3000);
		JsonNode hits = client.findAllByMarker(marker);
		assertNotNull(hits);
		assertEquals(1, hits.size());
		assertFieldValue(hits.get(0).get("_source"), "message", "0-" + test);

		// Appending 4 more events should not send it
		marker = getUniqueMarker(); // refresh marker for ease of finding
		for (int i = 1; i < 5; i++) {
			logger.error(MarkerManager.getMarker(marker), i + "-" + test);
		}
		doc = client.findFirstByMarker(marker);
		assertNull(doc);
		
		// But after the 5th event it should send all buffered events
		logger.error(MarkerManager.getMarker(marker), "5-" + test);
		hits = client.findAllByMarker(marker);
		assertNotNull(hits);
		assertEquals(5, hits.size());
		for (int i = 0; i < 5; i++) {
			assertFieldValue(hits.get(i).get("_source"), "message", (i + 1) + "-" + test);
		}
	}

}
