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

import java.net.InetAddress;
import org.apache.http.HttpHost;
import org.apache.logging.log4j.core.config.Node;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginBuilderAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginBuilderFactory;
import org.apache.logging.log4j.core.config.plugins.validation.constraints.Required;
import org.apache.logging.log4j.core.config.plugins.validation.constraints.ValidHost;
import org.apache.logging.log4j.core.config.plugins.validation.constraints.ValidPort;

/**
 * Plugin to hold an http address.
 *
 * @see HttpHost
 */
@Plugin(name = "HttpAddress", category = Node.CATEGORY, printObject = true)
public class HttpAddress {
	
	public static int DEFAULT_PORT = 9200;
	
    private HttpHost httpHost;

    private HttpAddress(final InetAddress host, final int port, final String scheme) {
    	this.httpHost = new HttpHost(host, port, scheme);
    }

    public HttpHost getHttpHost() {
    	return httpHost;
    }

    public int getPort() {
        return httpHost.getPort();
    }

    public InetAddress getAddress() {
        return httpHost.getAddress();
    }

    public String getHostName() {
        return httpHost.getHostName();
    }

    public String getSchemeName() {
        return httpHost.getSchemeName();
    }

    @PluginBuilderFactory
    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder implements org.apache.logging.log4j.core.util.Builder<HttpAddress> {

        @PluginBuilderAttribute
        private String scheme = "http";

        @PluginBuilderAttribute
        @ValidHost
        @Required(message = "Host address is required")
        private InetAddress host = InetAddress.getLoopbackAddress();

        @PluginBuilderAttribute
        @ValidPort
        private int port = 9200;

        public Builder withScheme(final String scheme) {
            this.scheme = scheme;
            return this;
        }
        
        public Builder withHost(final InetAddress host) {
            this.host = host;
            return this;
        }

        public Builder withPort(final int port) {
            this.port = port;
            return this;
        }

        @Override
        public HttpAddress build() {
            return new HttpAddress(host, port, scheme);
        }
    }

    @Override
    public String toString() {
        return httpHost.toString();
    }

}