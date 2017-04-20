#
# Zombo Dockerfile
#
# Based on Elasticsearch Dockerfile:
# https://github.com/dockerfile/elasticsearch
#

# Pull base image.
FROM java:7

ENV ES_PKG_NAME elasticsearch-1.7.5
ENV ZOMBODB_VER 3.1.13

# Install Elasticsearch.
RUN \
  cd / && \
  wget https://download.elastic.co/elasticsearch/elasticsearch/$ES_PKG_NAME.tar.gz && \
  tar xvzf $ES_PKG_NAME.tar.gz && \
  rm -f $ES_PKG_NAME.tar.gz && \
  mv /$ES_PKG_NAME /elasticsearch

# Define mountable directories.
VOLUME ["/data"]

# Mount elasticsearch.yml config
ADD config/elasticsearch.yml /elasticsearch/config/elasticsearch.yml

# Install zombo
RUN \
  cd / && \
  mkdir zombodb && \
  cd zombodb && \
  wget https://github.com/zombodb/zombodb/releases/download/v$ZOMBODB_VER/zombodb-es-plugin-$ZOMBODB_VER.zip

RUN /elasticsearch/bin/plugin -i zombodb -u file:///zombodb/zombodb-es-plugin-$ZOMBODB_VER.zip

# Define working directory.
WORKDIR /data

# Define default command.
CMD ["/elasticsearch/bin/elasticsearch"]

# Expose ports.
#   - 9200: HTTP
#   - 9300: transport
EXPOSE 9200
EXPOSE 9300
