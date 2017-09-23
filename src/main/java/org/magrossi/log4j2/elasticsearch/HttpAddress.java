package org.magrossi.log4j2.elasticsearch;

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