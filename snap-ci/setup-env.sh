#! /bin/sh

cat << DONE > /etc/yum.repos.d/elasticsearch.repo
[elasticsearch-1.7]
name=Elasticsearch repository for 1.7.x packages
baseurl=http://packages.elastic.co/elasticsearch/1.7/centos
gpgcheck=1
gpgkey=http://packages.elastic.co/GPG-KEY-elasticsearch
enabled=1
DONE

yum -y install elasticsearch

cat << DONE >> /etc/elasticsearch/elasticsearch.yml
script.disable_dynamic: false
threadpool.bulk.queue_size: 10
threadpool.bulk.size: 2
http.max_content_length: 1024mb
index.query.bool.max_clause_count: 1000000
DONE


