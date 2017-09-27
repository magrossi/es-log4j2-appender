package com.github.magrossi.log4j2.elasticsearch;

import java.io.IOException;
import java.util.Collections;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpResponseException;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.logging.log4j.util.Strings;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

public class ElasticBulkSender implements BulkSender {

	private static final String HTTP_METHOD = "POST";

	private final String user;
	private final String password;
	private final HttpHost[] hosts;
	private final RestClient restClient;

	public ElasticBulkSender(String user, String password, HttpHost... hosts) {
		this.user = user;
		this.password = password;
		this.hosts = hosts;
		this.restClient = RestClient.builder(hosts)
				.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
					@Override
					public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
						if (!Strings.isBlank(user)) {
							CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
							credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(user, password));
			                return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
						} else {
							return httpClientBuilder;
						}
					}
		        })
				.build();
	}

	@Override
	public void send(String body) throws IOException {
		HttpEntity entity = new NStringEntity(body.toString(), ContentType.APPLICATION_JSON);
		Response response = this.restClient.performRequest(HTTP_METHOD, "_bulk", Collections.emptyMap(), entity);
		if (response.getStatusLine().getStatusCode() >= 300) {
			throw new HttpResponseException(response.getStatusLine().getStatusCode(), response.getStatusLine().getReasonPhrase());
		}
	}
	
	public String getUser() {
		return user;
	}

	public String getPassword() {
		return password;
	}

	public HttpHost[] getHosts() {
		return hosts;
	}

	public RestClient getRestClient() {
		return this.restClient;
	}

}
