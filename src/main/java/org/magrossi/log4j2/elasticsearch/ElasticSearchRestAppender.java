package org.magrossi.log4j2.elasticsearch;

import java.io.Serializable;
import java.security.InvalidParameterException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.locks.*;

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
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.appender.AppenderLoggingException;
import org.apache.logging.log4j.core.config.plugins.*;
import org.apache.logging.log4j.core.layout.JsonLayout;
import org.apache.logging.log4j.status.StatusLogger;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import static org.apache.logging.log4j.core.Appender.ELEMENT_TYPE;
import static org.apache.logging.log4j.core.Core.CATEGORY_NAME;

/**
 * ElasticSearch REST Log4J appender
 */
@Plugin(name = "ElasticSearch", category = CATEGORY_NAME, elementType = ELEMENT_TYPE, printObject = true)
public class ElasticSearchRestAppender extends AbstractAppender {

    private static final Logger LOGGER = StatusLogger.getLogger();
	private static final String HTTP_METHOD = "POST";
    private final Lock lock = new ReentrantLock();
    private final RestClient restClient;
    private final String index;
    private final String type;
    private final DateFormat dateFormat;
    private final String bulkItemFormat;
    private final int maxBulkSize;
    private Timer timer;
    private final long maxDelayTime;
    private final List<String> buffered;
    
    /**
     * @param name The appender name
     * @param filter The appender filter
     * @param ignoreExceptions True if we are to ignore exceptions during logging
     * @param jsonMessages Flag indicating that the log event message is a valid JSON string
     * @param maxDelayTime Max delay time before sending the messages to the database
     * @param maxBulkSize Max buffer size of messages held in memory before sending
     * @param dateFormat Format of the timestamp that is appended to the index name while saving
     * @param index The ElasticSearch destination index
     * @param type The ElasticSearch destination type
     * @param hosts List of ElasticSearch hosts in the cluster
     */
    protected ElasticSearchRestAppender(String name, Filter filter, Layout<? extends Serializable> layout, final boolean ignoreExceptions,
    		final long maxDelayTime, final int maxBulkSize, DateFormat dateFormat,
    		String index, String type, String user, String password, HttpHost... hosts) {
        super(name, filter, initLayout(layout), ignoreExceptions);
        this.buffered = new ArrayList<>();
        this.timer = null;
        this.maxBulkSize = maxBulkSize;
        this.maxDelayTime = maxDelayTime;
        this.index = index;
        this.type = type;
        this.dateFormat = dateFormat;
        this.bulkItemFormat = String.format("{ \"index\" : { \"_index\" : \"%s%%s\", \"_type\" : \"%s\" } }%n%%s%n", index, type);
        this.restClient = initClient(user, password, hosts);
    }
    
    private static RestClient initClient(String user, String password, HttpHost... hosts) {
    	return RestClient.builder(hosts)
				.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
					@Override
					public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
						if (user != null && !user.trim().isEmpty()) {
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
    
    private static Layout<? extends Serializable> initLayout(Layout<? extends Serializable> layout) {
        if (layout != null) {
        	if (layout.getContentType().toLowerCase().contains("application/json")) {
        		return layout;
        	} else {
        		throw new InvalidParameterException("Layout must produce an \"application/json\" content type. "
        										  + "Instead it produces \"" + layout.getContentType() + "\"");
        	}
        } else {
        	return JsonLayout.createDefaultLayout();
        }
    }
    
    /**
     * @param jsonMessage The json parsed message
     * @return The full action metadata including timestamp
     */
    private String getBulkItem(String jsonMessage) {
   		return String.format(bulkItemFormat, this.dateFormat.format(new Date()), jsonMessage);
    }
    
    /**
     * @see org.apache.logging.log4j.core.Appender#append(org.apache.logging.log4j.core.LogEvent)
     */
    @Override
    public void append(LogEvent event) {
    	lock.lock();
        try {
        	// Parse event and adds to buffer
        	String json = new String(getLayout().toByteArray(event));
        	buffered.add(getBulkItem(json));
        	// Check timer and buffer size
        	this.checkSendToEs();        	
        } catch (Exception ex) {
            if (!ignoreExceptions()) {
                throw new AppenderLoggingException(ex);
            }
        } finally {
        	lock.unlock();
        }
    }
    
    /**
     * Checks if it should send the messages to the remote server
     */
    private void checkSendToEs() {
    	// Send if: buffered.size() >= maxBulkSize
    	//      or: if timer == null, start timer
    	if (buffered.size() >= this.maxBulkSize) {
    		// Cancels timer
    		if (timer != null) {
    			timer.cancel();
    			timer = null;
    		}
    		// Sends to remote
    		this.sendToEs();
    	} else if (timer == null) {
    		// create timer
    		timer = new Timer();
    		timer.schedule(new TimerTask() {				
				@Override
				public void run() {
					lock.lock();
					try {
						sendToEs();
						timer.cancel();
						timer = null;
					} finally {
						lock.unlock();
					}
				}
			}, this.maxDelayTime);
    	}
    }
    
    /**
     * Send buffered messages to elastic
     */
    private void sendToEs() {
    	try {
    		if (buffered.size() > 0) {
	    		StringBuilder bulkRequestBody = new StringBuilder();
	    		for (String bulkItem : buffered) {
	    		    bulkRequestBody.append(bulkItem);
	    		}
	        	HttpEntity entity = new NStringEntity(bulkRequestBody.toString(), ContentType.APPLICATION_JSON);
	        	try {
					Response response = this.restClient.performRequest(HTTP_METHOD, "_bulk", Collections.emptyMap(), entity);
					if (response.getStatusLine().getStatusCode() >= 300) {
						throw new HttpResponseException(response.getStatusLine().getStatusCode(), response.getStatusLine().getReasonPhrase());
					}
				} catch (Exception ex) {
		            if (!ignoreExceptions()) {
		                throw new AppenderLoggingException(ex);
		            } else {
		            	LOGGER.error("Failed to send data to ElasticSearch server.", ex);
		            }
				}
    		}
    	} finally {
    		buffered.clear();
    	}
    }
    
    /**
     * @see org.apache.logging.log4j.core.AbstractLifeCycle#stop()
     */
    @Override
    public void stop() {
    	// cancel the timer
    	this.timer.cancel();
    	// try to send the current buffer
    	this.sendToEs();
    	// then we stop
    	super.stop();
    }

    /**
     * @param hosts List of comma (or semicolon) separated hostnames with port
     * @return The array of hosts
     */
    protected static HttpHost[] parseHosts(String hosts) {
		ArrayList<HttpHost> hostList = new ArrayList<>();
    	if (hosts != null && !hosts.trim().isEmpty()) {
    		String[] hostArray = hosts.split(",|;");
    		for (String hostString : hostArray) {
    			if (!hostString.trim().isEmpty()) {
    				try {
    					hostList.add(HttpHost.create(hostString.trim()));
    				} catch (Exception e) {
    					LOGGER.warn("Ignoring invalid host for ElasticSearchRestAppender " + hostString.trim());
					}
    			}
    		}
    	}
    	
    	if (hostList.size() == 0) {
    		return null;
    	} else {
    		return hostList.toArray(new HttpHost[hostList.size()]);
    	}

    }

    /**
	 * @return the index
	 */
	protected String getIndex() {
		return index;
	}

	/**
	 * @return the type
	 */
	protected String getType() {
		return type;
	}

	/**
	 * @return the dateFormat
	 */
	protected DateFormat getDateFormat() {
		return dateFormat;
	}

	/**
	 * @return the maxBulkSize
	 */
	protected int getmaxBulkSize() {
		return maxBulkSize;
	}

	/**
	 * @return the maxDelayTime
	 */
	protected long getMaxDelayTime() {
		return maxDelayTime;
	}

	/**
   	 * @param name The appender name 
	 * @param filter The appender filter
	 * @param jsonMessages Flag indicating that the log event message is a valid JSON string
     * @param maxBulkSize Max buffer size of messages held in memory before sending
     * @param maxDelayTime Max delay time before sending the messages to the database
     * @param esIndex The ElasticSearch destination index
     * @param dateFormat Format of the timestamp that is appended to the index name while saving
     * @param esType The ElasticSearch destination type
     * @param esHosts Comma or semicolon separated list of ElasticSearch hosts in the cluster (ie. "http://node01.escluster.com:9200,http://node02.escluster.com:9200")
     * @return The appender
     */
    @PluginFactory
    public static ElasticSearchRestAppender createAppender(
            @PluginAttribute("name") String name,
            @PluginElement("Filter") final Filter filter,
            @PluginElement("Layout") Layout<? extends Serializable> layout,
            @PluginAttribute("jsonMessages") Boolean jsonMessages,
            @PluginAttribute("maxBulkSize") Integer maxBulkSize,
            @PluginAttribute("maxDelayTime") Long maxDelayTime,
            @PluginAttribute("index") String esIndex,
            @PluginAttribute("dateFormat") String dateFormat,
            @PluginAttribute("type") String esType,
            @PluginAttribute("hosts") String esHosts) {

        if (name == null) {
            LOGGER.error("No name provided for ElasticSearchRestAppender");
            return null;
        }
        
        if (maxBulkSize == null) {
        	maxBulkSize = 200;
        }
        
        if (maxDelayTime == null) {
        	maxDelayTime = 2000L;
        }
        
        if (jsonMessages == null) {
        	jsonMessages = false;
        }

        HttpHost[] hosts = parseHosts(esHosts);
        if (hosts == null) {
			LOGGER.warn("No hosts found for appender {} using [http://localhost:9200].", name);
        	hosts = new HttpHost[] { new HttpHost("localhost", 9200) };
        }
        
        if (esIndex == null || esIndex.trim().isEmpty()) {
        	esIndex = "logs-";
        }
        
        if (dateFormat == null) {
        	dateFormat = "yyyyMMdd";        	
        }
        
        if (esType == null || esType.trim().isEmpty()) {
        	esType = "log";
        }
        
        if (layout == null) {
        	layout = JsonLayout.createDefaultLayout();
        }

        return new ElasticSearchRestAppender(name, filter, layout, true, maxDelayTime,
        		maxBulkSize, new SimpleDateFormat(dateFormat), esIndex, esType, esHosts, esHosts, hosts);
    }
    
}