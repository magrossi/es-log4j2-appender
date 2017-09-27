package com.github.magrossi.log4j2.elasticsearch;

import java.io.IOException;

public interface BulkSender {
	void send(String body) throws IOException;
}
