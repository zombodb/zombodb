#
# Postgres with ZomboDB
#

# Pull base image.
FROM postgres:9.3

# Fetch wget
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates wget && rm -rf /var/lib/apt/lists/*

# Install zombo
RUN \
  cd / && \
  mkdir zombodb && \
  cd zombodb && \
  wget https://github.com/zombodb/zombodb/releases/download/v2.1.38/zombodb_trusty_pg93-2.1.38_amd64.deb

RUN \
  cd /zombodb && \
  dpkg -i zombodb_trusty_pg93-2.1.38_amd64.deb

COPY zombo-entrypoint.sh /

RUN chmod +x /zombo-entrypoint.sh

ENTRYPOINT ["/zombo-entrypoint.sh"]

CMD ["postgres"]