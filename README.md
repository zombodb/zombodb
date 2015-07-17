Product | Version | Comments
--- | --- | --- |
[Postgres][1] | 9.3.4 |
[elasticsearch.][5]| 1.5.3
Java JDK | 1.7.0_51+ | |
libCurl | 7.37.1+ | |
Standard C build environment | |
Apache Maven | 3.0.5 ||

```yaml
cluster.name: <your unique clustername>
http.max_content_length: 1024mb
http.compression: true
threadpool.bulk.queue_size: 1024
threadpool.bulk.size: 12
script.disable_dynamic: false
index.query.bool.max_clause_count: 1000000
indices.store.throttle.max_bytes_per_sec: 1024mb
```
