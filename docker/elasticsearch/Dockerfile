#
# Zombo Dockerfile
#
# Based on Elasticsearch Dockerfile:
# https://github.com/dockerfile/elasticsearch
#

# Pull base image.
FROM java:8

ENV ES_PKG_NAME elasticsearch-5.6.4
ENV ZOMBODB_VER 5.6.4-1.0.1

# need a non-root user to run elasticsearch
RUN useradd -ms /bin/bash elastic

# Install Elasticsearch.
RUN \
  cd / && \
  wget -qO - https://artifacts.elastic.co/downloads/elasticsearch/${ES_PKG_NAME}.tar.gz | tar xzvf -
RUN \
  mv ${ES_PKG_NAME} elasticsearch && \
  chown -R elastic:elastic elasticsearch

# Define mountable directories.
RUN mkdir -p /data/data /data/logs
RUN chown -R elastic:elastic /data
RUN ls -la /data
VOLUME ["/data"]

# become the elasticsearch user
USER elastic

# Mount elasticsearch.yml config
ADD config/elasticsearch.yml /elasticsearch/config/elasticsearch.yml

# Install zombo
RUN /elasticsearch/bin/elasticsearch-plugin install https://www.zombodb.com/releases/v$ZOMBODB_VER/zombodb-es-plugin-$ZOMBODB_VER.zip

# Define working directory.
WORKDIR /data

# Define default command.
CMD ["/elasticsearch/bin/elasticsearch"]

# Expose ports.
#   - 9200: HTTP
#   - 9300: transport
EXPOSE 9200
EXPOSE 9300
