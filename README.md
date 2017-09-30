# es-log4j2-appender [![travis](https://travis-ci.org/magrossi/es-log4j2-appender.svg?branch=master)](https://travis-ci.org/magrossi/es-log4j2-appender) [![codecov](https://codecov.io/gh/magrossi/es-log4j2-appender/branch/master/graph/badge.svg)](https://codecov.io/gh/magrossi/es-log4j2-appender)
An ElasticSearch REST appender for Log4j2

This is a simple appender that sends your log data JSON formatted directly to ElasticSearch via the REST API. There are options to buffer the logs before sending in bulk. Two options are provided for the buffering of logs.
- Max Bulk Size: when set to a value greater than `0` it will buffer messages until `maxBulkSize` is reached, when it will then send the whole lot using ElasticSearch's `_bulk` API (defaults to `200`).
- Max Delay Time: similarly to the previous option, `maxDelayTime` will accumulate log messages for up to `maxDelayTime` in milliseconds (counted from the first message received) and it will send the entirety of the accumulated messages in a single bulk (defaults to `2000`ms).
Any combination of the two options can be used. Setting any to `0` effectively disables it.

The appender uses the `JSONLayout` by default, but a custom layout can be provided. The only requirement is that the layout produces an `application/json` content type.
