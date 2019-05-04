# ElasticSearch Log4j2 Appender

[![Build Status](https://travis-ci.org/magrossi/es-log4j2-appender.svg?branch=master)](https://travis-ci.org/magrossi/es-log4j2-appender)
[![Code Coverage](https://codecov.io/gh/magrossi/es-log4j2-appender/branch/master/graph/badge.svg)](https://codecov.io/gh/magrossi/es-log4j2-appender)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.magrossi/log4j2-elasticsearch-appender/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.github.magrossi/log4j2-elasticsearch-appender)

An ElasticSearch REST appender for Log4j2

This is a simple appender that sends your log data JSON formatted directly to ElasticSearch via the REST API. There are options to buffer the logs before sending in bulk. Two options are provided for the buffering of logs.
- **Max Bulk Size:** when set to a value greater than `0` it will buffer messages until `maxBulkSize` is reached, when it will then send the whole lot using ElasticSearch's `_bulk` API (defaults to `200`).
- **Max Delay Time:** similarly to the previous option, `maxDelayTime` will accumulate log messages for up to `maxDelayTime` in milliseconds (counted from the first message received) and it will send the entirety of the accumulated messages in a single bulk (defaults to `2000`ms).

Any combination of the two options can be used. Setting any to `0` effectively disables it, if both are set to `0` logs are sent one by one as soon as they are received.

The appender uses the `JSONLayout` by default, but a custom layout can be provided. The only requirement is that the layout produces an `application/json` content type.

To use it, simply reference this package into your Log4j2 configuration file, and add the appender with as your ElasticSearch nodes as hosts and you're good to go!
```xml
<Configuration status="debug" strict="true" name="ElasticSearchAppenderTest"
               packages="com.github.magrossi.log4j2.elasticsearch">
    <Appenders>
        <!-- Sends logs immediately -->
        <Appender type="ElasticSearch"
                  <!-- Your appender name -->
                  name="ELASTIC"
                  <!-- Accumulate up to "maxBulkSize" before sending the logs -->
                  maxBulkSize="0" 
                  <!-- Waits maxDelayTime (in millis) before sending the logs -->
                  maxDelayTime="0"
                  <!-- ElasticSearch index/type configuration -->
                  esIndex="my-index-"
                  esType="logs"
                  <!-- The index name is actually {index}{dateFormat}, so if
                       esIndex="my-index-", and
                       dateFormat="yyyyMMdd" (and the current date is 01/01/2001
                       Then the ElasticSearch index will be resolved to "my-index-20010101" -->
                  dateFormat="yyyyMMdd"
                  <!-- ElasticSearch credentials, if required -->
                  user="your-username"
                  password="your-password">
            <!-- List of nodes in your ElasticSearch cluster -->
            <Host type="HttpAddress" scheme="http" host="my-node-1" port="9200"/>
            <Host type="HttpAddress" scheme="http" host="my-node-2" port="9200"/>
        </Appender>
    </Appenders>
    <Loggers>
        <Logger name="ELASTIC_LOGGER" level="debug" additivity="false">
            <AppenderRef ref="ELASTIC"/>
        </Logger>
    </Loggers>
</Configuration>
```