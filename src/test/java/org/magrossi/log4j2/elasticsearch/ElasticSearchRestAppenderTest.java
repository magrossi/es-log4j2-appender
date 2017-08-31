package org.magrossi.log4j2.elasticsearch;

import static org.junit.Assert.*;

import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.impl.Log4jLogEvent;
import org.junit.Test;


public class ElasticSearchRestAppenderTest {

	@Test
	public void test() {
		final LoggerContext loggerContext = Configurator.initialize("Test Configuration", "src/test/resources/test.properties");
		Appender appender = loggerContext.getConfiguration().getAppender("ES");
		assertNotNull(appender);
		appender.append(new Log4jLogEvent());
	}

}
