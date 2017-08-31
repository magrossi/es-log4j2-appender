package org.magrossi.log4j2.elasticsearch;

import static org.junit.Assert.*;

import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.Test;


public class ElasticSearchRestAppenderTest {

	@Test
	public void test() {
		final LoggerContext loggerContext = Configurator.initialize("Test Configuration", "src/test/resources/test.properties");
		Logger logger = loggerContext.getLogger("test_logger");
		assertNotNull(logger);
	}

}
