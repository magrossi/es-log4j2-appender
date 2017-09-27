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

import java.io.Serializable;
import java.nio.charset.Charset;
import java.security.InvalidParameterException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.locks.*;
import java.util.stream.Collectors;

import org.apache.http.HttpHost;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.appender.AppenderLoggingException;
import org.apache.logging.log4j.core.config.plugins.*;
import org.apache.logging.log4j.core.config.plugins.validation.constraints.Required;
import org.apache.logging.log4j.core.layout.JsonLayout;
import org.apache.logging.log4j.util.Strings;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import static org.apache.logging.log4j.core.Appender.ELEMENT_TYPE;
import static org.apache.logging.log4j.core.Core.CATEGORY_NAME;

/**
 * Elastic REST Log4J2 appender that sends documents in bulk.
 * Log messages are buffered and sent at pre-defined interval or
 * when the message buffer gets filled (whichever comes first). * 
 */
@Plugin(name = "ElasticSearch", category = CATEGORY_NAME, elementType = ELEMENT_TYPE, printObject = true)
public class ElasticSearchRestAppender extends AbstractAppender {

    public static class Builder<B extends Builder<B>> extends AbstractAppender.Builder<B>
		implements org.apache.logging.log4j.core.util.Builder<AbstractAppender> {

        @PluginBuilderAttribute
        @Required(message = "No esIndex provided")
        private String esIndex;
        
        @PluginBuilderAttribute
        private String dateFormat;

        @PluginBuilderAttribute
        @Required(message = "No esType provided")
        private String esType;

        @PluginBuilderAttribute
        private String user;
        
        @PluginBuilderAttribute
        private String password;

        @PluginBuilderAttribute
        private Integer maxBulkSize;
        
        @PluginBuilderAttribute
        private Long maxDelayTime;
        
        @PluginElement("Hosts")
        @Required(message = "No Elastic hosts provided")
        private HttpAddress[] hosts;

        private BulkSender bulkSender;

        public B withIndex(final String index) {
            this.esIndex = index;
            return asBuilder();
        }
        
        public B withDateFormat(final String dateFormat) {
            this.dateFormat = dateFormat;
            return asBuilder();
        }
        
        public B withType(final String type) {
            this.esType = type;
            return asBuilder();
        }

        public B withHosts(final HttpAddress... hosts) {
            this.hosts = hosts;
            return asBuilder();
        }

        public B withMaxBulkSize(final Integer maxBulkSize) {
            this.maxBulkSize = maxBulkSize;
            return asBuilder();
        }
        
        public B withMaxDelayTime(final Long maxDelayTime) {
            this.maxDelayTime = maxDelayTime;
            return asBuilder();
        }
        
        public B withCredentials(final String user, final String password) {
        	this.user = user;
        	this.password = password;
        	return asBuilder();
        }
        
        public B withBulkSender(final BulkSender bulkSender) {
        	this.bulkSender = bulkSender;
        	return asBuilder();
        }
        
    	@Override
    	public Layout<? extends Serializable> getOrCreateLayout() {
    		return getOrCreateLayout(Charset.defaultCharset());
    	}

    	@Override
    	public Layout<? extends Serializable> getOrCreateLayout(final Charset charset) {
            if (getLayout() == null) {
                return JsonLayout.newBuilder()
            			.setCompact(true)
            			.setCharset(charset)
            			.setIncludeStacktrace(true)
            			.setLocationInfo(true)
            			.setProperties(true)
            			.build();
            }
            return getLayout();
        }
        
        @Override
        public ElasticSearchRestAppender build() {

            if (this.getName() == null) {
                LOGGER.error("No name provided for ElasticSearchRestAppender");
                return null;
            }

            if (maxBulkSize == null || maxBulkSize < 0) {
            	maxBulkSize = 200;
            }
            
            if (maxDelayTime == null || maxDelayTime < 0) {
            	maxDelayTime = 2000L;
            }

            HttpHost[] httpHosts;
            if (hosts == null || hosts.length == 0) {
    			LOGGER.warn("No hosts found for appender {} using [http://localhost:9200].", getName());
    			httpHosts = new HttpHost[] { new HttpHost("localhost", 9200) };
            } else {
            	httpHosts = Arrays.asList(hosts).stream()
            			.map(HttpAddress::getHttpHost)
            			.collect(Collectors.toList())
            			.toArray(new HttpHost[hosts.length]);
            }
            
            if (Strings.isBlank(esIndex)) {
            	LOGGER.warn("No esIndex found for appender {} using [logs-].", getName());
            	esIndex = "logs-";
            }
            
            if (dateFormat == null) {
            	LOGGER.warn("No date format found for appender {} using [yyyyMMdd].", getName());
            	dateFormat = "yyyyMMdd";
            }
            
            if (Strings.isBlank(esType)) {
            	LOGGER.warn("No esType found for appender {} using [log].", getName());
            	esType = "log";
            }
            
            if (bulkSender == null) {
            	bulkSender = new ElasticBulkSender(user, password, httpHosts);
            } else {
            	LOGGER.warn("Appender {} using custom bulk sender {}.", getName(), bulkSender.getClass().getName());
            }

            return new ElasticSearchRestAppender(getName(), getFilter(), getOrCreateLayout(), isIgnoreExceptions(),
            		maxDelayTime, maxBulkSize, new SimpleDateFormat(dateFormat), esIndex, esType, bulkSender);
        }

    }
	
    @PluginBuilderFactory
    public static <B extends Builder<B>> B newBuilder() {
        return new Builder<B>().asBuilder();
    }
    
    private final Lock lock = new ReentrantLock();
    private final BulkSender bulkSender;
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
     * @param maxDelayTime Max delay time before sending the messages to the database
     * @param maxBulkSize Max buffer size of messages held in memory before sending
     * @param dateFormat Format of the timestamp that is appended to the esIndex name while saving
     * @param esIndex The ElasticSearch destination esIndex
     * @param esType The ElasticSearch destination esType
     * @param hosts List of ElasticSearch hosts in the cluster
     */
    protected ElasticSearchRestAppender(String name, Filter filter, Layout<? extends Serializable> layout, final boolean ignoreExceptions,
    		final long maxDelayTime, final int maxBulkSize, DateFormat dateFormat,
    		String index, String type, BulkSender bulkSender) {
        super(name, filter, layout, ignoreExceptions);
        this.buffered = new ArrayList<>();
        this.timer = null;
        this.maxBulkSize = maxBulkSize;
        this.maxDelayTime = maxDelayTime;
        this.index = index;
        this.type = type;
        this.bulkSender = bulkSender;
        this.dateFormat = dateFormat;
        this.bulkItemFormat = String.format("{ \"index\" : { \"_index\" : \"%s%%s\", \"_type\" : \"%s\" } }%n%%s%n", index, type);
        this.validate();
    }
    
    private void validate() {
        if (getLayout() != null) {
        	if (!getLayout().getContentType().toLowerCase().contains("application/json")) {
        		throw new InvalidParameterException("Layout must produce an \"application/json\" content esType. "
        										  + "Instead it produces \"" + getLayout().getContentType() + "\"");
        	}
        } else {
    		throw new InvalidParameterException("Layout not provided");
        }
    }
    
    private String getBulkItem(String jsonMessage) {
   		return String.format(bulkItemFormat, this.dateFormat.format(new Date()), jsonMessage);
    }
    
    @Override
    public void append(LogEvent event) {
    	lock.lock();
        try {
        	String json = new String(getLayout().toByteArray(event));
        	buffered.add(getBulkItem(json));
        	this.check();        	
        } catch (Exception ex) {
            if (!ignoreExceptions()) {
                throw new AppenderLoggingException(ex);
            } else {
            	LOGGER.error("Failed to process event.", ex);
            }
        } finally {
        	lock.unlock();
        }
    }

    private void cancelTimer() {
		if (timer != null) {
			timer.cancel();
			timer = null;
		}
    }
    
    private void check() {
    	if (this.maxBulkSize == 0 && this.maxDelayTime == 0) {
    		send();
    	} else if (this.maxBulkSize > 0 && buffered.size() >= this.maxBulkSize) {
    		send();
    	} else if (this.maxDelayTime > 0 && timer == null) {
    		timer = new Timer();
    		timer.schedule(new TimerTask() {				
				@Override
				public void run() {
					lock.lock();
					try {
						send();
					} finally {
						lock.unlock();
					}
				}
			}, this.maxDelayTime);
    	}
    }
    
    private void send() {
    	try {
			cancelTimer();
    		if (buffered.size() > 0) {
	    		StringBuilder bulkRequestBody = new StringBuilder();
	    		for (String bulkItem : buffered) {
	    		    bulkRequestBody.append(bulkItem);
	    		}
	        	try {
	        		this.bulkSender.send(bulkRequestBody.toString());
				} catch (Exception ex) {
		            if (!ignoreExceptions()) {
		                throw new AppenderLoggingException(ex);
		            } else {
		            	LOGGER.error("Failed to send data to Elastic server.", ex);
		            }
				}
    		}
    	} finally {
    		buffered.clear();
    	}
    }
    
    @Override
    public void stop() {
    	this.timer.cancel();
    	this.send();
    	super.stop();
    }

    protected String getIndex() {
		return index;
	}

	protected String getType() {
		return type;
	}

	protected DateFormat getDateFormat() {
		return dateFormat;
	}

	protected int getmaxBulkSize() {
		return maxBulkSize;
	}

	protected long getMaxDelayTime() {
		return maxDelayTime;
	}

	public BulkSender getBulkSender() {
		return bulkSender;
	}

	public int getMaxBulkSize() {
		return maxBulkSize;
	}

}