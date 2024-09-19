---
title: "Prometheus时序数据采集"
excerpt: "Prometheus时序数据实时采集"
categories:
  - prometheus
tags:
  - 时序
---

# 目录
{% include toc %}

基于Prometheus的remote_write 特性的数据采集

![hbase](/images/blog/prometheus/prometheus_colllect.png)

## 收集后端
1. proto定义，来源于Prometheus源码
remote.proto

```
// Copyright 2016 Prometheus Team
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

syntax = "proto3";
package prometheus;

option go_package = "prompb";

import "types.proto";
import "gogoproto/gogo.proto";

message WriteRequest {
  repeated prometheus.TimeSeries timeseries = 1 [(gogoproto.nullable) = false];
}

message ReadRequest {
  repeated Query queries = 1;
}

message ReadResponse {
  // In same order as the request's queries.
  repeated QueryResult results = 1;
}

message Query {
  int64 start_timestamp_ms = 1;
  int64 end_timestamp_ms = 2;
  repeated prometheus.LabelMatcher matchers = 3;
  prometheus.ReadHints hints = 4;
}

message QueryResult {
  // Samples within a time series must be ordered by time.
  repeated prometheus.TimeSeries timeseries = 1;
}

```

types.proto

```
// Copyright 2017 Prometheus Team
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

syntax = "proto3";
package prometheus;

option go_package = "prompb";

import "gogoproto/gogo.proto";

message Sample {
  double value    = 1;
  int64 timestamp = 2;
}

message TimeSeries {
  repeated Label labels   = 1 [(gogoproto.nullable) = false];
  repeated Sample samples = 2 [(gogoproto.nullable) = false];
}

message Label {
  string name  = 1;
  string value = 2;
}

message Labels {
  repeated Label labels = 1 [(gogoproto.nullable) = false];
}

// Matcher specifies a rule, which can match or set of labels or not.
message LabelMatcher {
  enum Type {
    EQ  = 0;
    NEQ = 1;
    RE  = 2;
    NRE = 3;
  }
  Type type    = 1;
  string name  = 2;
  string value = 3;
}

message ReadHints {
  int64 step_ms = 1;  // Query step size in milliseconds.
  string func = 2;    // String representation of surrounding function or aggregation.
  int64 start_ms = 3; // Start time in milliseconds.
  int64 end_ms = 4;   // End time in milliseconds.
}

```

2. 创建/write接口

```java
package io.prometheus.remote;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.xerial.snappy.Snappy;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;

import io.prometheus.remote.kafka.KafkaTopicConfig;
import prometheus.Remote.WriteRequest;
import prometheus.Types.Label;
import prometheus.Types.Sample;
import prometheus.Types.TimeSeries;

@RestController
public class RemoteController {

	static Logger logger = LoggerFactory.getLogger(RemoteController.class);

//	@Autowired
	private KafkaTopicConfig kafkaTopicConfig;
	
//	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;
	
	@RequestMapping(method = RequestMethod.POST, value = "/write")
	public void write(@RequestBody byte[] byteArray) throws IOException {

		WriteRequest wr = WriteRequest.parseFrom(Snappy.uncompress(byteArray));
		
		 logger.info(JsonFormat.printer().print(wr));


		List<TimeSeries> tsl = wr.getTimeseriesList();

		for (TimeSeries ts : tsl) {
			Map<String, String> lm = ts.getLabelsList().stream()
					.collect(Collectors.toMap(Label::getName, Label::getValue));
			List<Sample> ls = ts.getSamplesList();

			for (Sample s : ls) {


				String timestamp = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX")
						.format(new Date(s.getTimestamp()));
				String name = lm.get("__name__");
				String value = String.valueOf(s.getValue());

				logger.info("Sample: timestamp=" + timestamp + ", name=" + name + ", value=" + value + ", labels=" + mapAsString(lm));

			}
		}

	}
	

	private String mapAsString(Map<String, String> map) {
	    String mapAsString = map.keySet().stream()
	      .map(key -> key + "=" + map.get(key))
	      .collect(Collectors.joining(", ", "{", "}"));
	    return mapAsString;
	}


}
```

## 新增收集配置
修改promethus.yml，新增remote_write的配置，如下的demo

```yml
global:
  scrape_interval:     15s # By default, scrape targets every 15 seconds.

  # Attach these labels to any time series or alerts when communicating with
  # external systems (federation, remote storage, Alertmanager).
  external_labels:
    monitor: 'codelab-monitor'

# A scrape configuration containing exactly one endpoint to scrape:
# Here it's Prometheus itself.
scrape_configs:
  # The job name is added as a label `job=<job_name>` to any timeseries scraped from this config.
  - job_name: 'prometheus'

    # Override the global default and scrape targets from this job every 5 seconds.
    scrape_interval: 5s

    static_configs:
      - targets: ['localhost:9090']


remote_write:
  - url: "http://host.docker.internal:8080/write"
```

## 收集样例

```log
2024-08-01 14:58:29 [INFO ] |io.prometheus.remote.RemoteController| - Sample: timestamp=2024-08-01T14:58:28+08:00, name=prometheus_wal_watcher_notifications_skipped_total, value=128.0, labels={instance=localhost:9090, __name__=prometheus_wal_watcher_notifications_skipped_total, monitor=codelab-monitor, job=prometheus, consumer=35dd99}
2024-08-01 14:58:29 [INFO ] |io.prometheus.remote.RemoteController| - Sample: timestamp=2024-08-01T14:58:28+08:00, name=prometheus_wal_watcher_record_decode_failures_total, value=0.0, labels={instance=localhost:9090, __name__=prometheus_wal_watcher_record_decode_failures_total, monitor=codelab-monitor, job=prometheus, consumer=35dd99}
2024-08-01 14:58:29 [INFO ] |io.prometheus.remote.RemoteController| - Sample: timestamp=2024-08-01T14:58:28+08:00, name=prometheus_wal_watcher_records_read_total, value=367.0, labels={instance=localhost:9090, __name__=prometheus_wal_watcher_records_read_total, monitor=codelab-monitor, type=samples, job=prometheus, consumer=35dd99}
2024-08-01 14:58:29 [INFO ] |io.prometheus.remote.RemoteController| - Sample: timestamp=2024-08-01T14:58:28+08:00, name=prometheus_wal_watcher_records_read_total, value=3.0, labels={instance=localhost:9090, __name__=prometheus_wal_watcher_records_read_total, monitor=codelab-monitor, type=series, job=prometheus, consumer=35dd99}
2024-08-01 14:58:29 [INFO ] |io.prometheus.remote.RemoteController| - Sample: timestamp=2024-08-01T14:58:28+08:00, name=prometheus_wal_watcher_samples_sent_pre_tailing_total, value=0.0, labels={instance=localhost:9090, __name__=prometheus_wal_watcher_samples_sent_pre_tailing_total, monitor=codelab-monitor, job=prometheus, consumer=35dd99}
2024-08-01 14:58:29 [INFO ] |io.prometheus.remote.RemoteController| - Sample: timestamp=2024-08-01T14:58:28+08:00, name=prometheus_web_federation_errors_total, value=0.0, labels={instance=localhost:9090, __name__=prometheus_web_federation_errors_total, monitor=codelab-monitor, job=prometheus}
2024-08-01 14:58:29 [INFO ] |io.prometheus.remote.RemoteController| - Sample: timestamp=2024-08-01T14:58:28+08:00, name=prometheus_web_federation_warnings_total, value=0.0, labels={instance=localhost:9090, __name__=prometheus_web_federation_warnings_total, monitor=codelab-monitor, job=prometheus}
2024-08-01 14:58:29 [INFO ] |io.prometheus.remote.RemoteController| - Sample: timestamp=2024-08-01T14:58:28+08:00, name=promhttp_metric_handler_requests_in_flight, value=1.0, labels={instance=localhost:9090, __name__=promhttp_metric_handler_requests_in_flight, monitor=codelab-monitor, job=prometheus}
2024-08-01 14:58:29 [INFO ] |io.prometheus.remote.RemoteController| - Sample: timestamp=2024-08-01T14:58:28+08:00, name=promhttp_metric_handler_requests_total, value=367.0, labels={instance=localhost:9090, code=200, __name__=promhttp_metric_handler_requests_total, monitor=codelab-monitor, job=prometheus}
2024-08-01 14:58:29 [INFO ] |io.prometheus.remote.RemoteController| - Sample: timestamp=2024-08-01T14:58:28+08:00, name=promhttp_metric_handler_requests_total, value=0.0, labels={instance=localhost:9090, code=500, __name__=promhttp_metric_handler_requests_total, monitor=codelab-monitor, job=prometheus}
2024-08-01 14:58:29 [INFO ] |io.prometheus.remote.RemoteController| - Sample: timestamp=2024-08-01T14:58:28+08:00, name=promhttp_metric_handler_requests_total, value=0.0, labels={instance=localhost:9090, code=503, __name__=promhttp_metric_handler_requests_total, monitor=codelab-monitor, job=prometheus}
2024-08-01 14:58:29 [INFO ] |io.prometheus.remote.RemoteController| - Sample: timestamp=2024-08-01T14:58:28+08:00, name=up, value=1.0, labels={instance=localhost:9090, __name__=up, monitor=codelab-monitor, job=prometheus}
2024-08-01 14:58:29 [INFO ] |io.prometheus.remote.RemoteController| - Sample: timestamp=2024-08-01T14:58:28+08:00, name=scrape_duration_seconds, value=0.010108583, labels={instance=localhost:9090, __name__=scrape_duration_seconds, monitor=codelab-monitor, job=prometheus}
2024-08-01 14:58:29 [INFO ] |io.prometheus.remote.RemoteController| - Sample: timestamp=2024-08-01T14:58:28+08:00, name=scrape_samples_scraped, value=586.0, labels={instance=localhost:9090, __name__=scrape_samples_scraped, monitor=codelab-monitor, job=prometheus}
2024-08-01 14:58:29 [INFO ] |io.prometheus.remote.RemoteController| - Sample: timestamp=2024-08-01T14:58:28+08:00, name=scrape_samples_post_metric_relabeling, value=586.0, labels={instance=localhost:9090, __name__=scrape_samples_post_metric_relabeling, monitor=codelab-monitor, job=prometheus}
2024-08-01 14:58:29 [INFO ] |io.prometheus.remote.RemoteController| - Sample: timestamp=2024-08-01T14:58:28+08:00, name=scrape_series_added, value=0.0, labels={instance=localhost:9090, __name__=scrape_series_added, monitor=codelab-monitor, job=prometheus}
```