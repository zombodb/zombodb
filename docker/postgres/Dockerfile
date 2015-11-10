#
# Postgres with ZomboDB
#

# Pull base image.
FROM postgres:9.3
ENV ZOMBODB_VER 2.5.1

# Fetch wget
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates wget && rm -rf /var/lib/apt/lists/*

# Install zombo
RUN \
  cd / && \
  mkdir zombodb && \
  cd zombodb && \
  wget https://github.com/zombodb/zombodb/releases/download/v${ZOMBODB_VER}/zombodb_trusty_pg93-${ZOMBODB_VER}_amd64.deb

RUN \
  cd /zombodb && \
  dpkg -i zombodb_trusty_pg93-${ZOMBODB_VER}_amd64.deb

COPY zombodb-entrypoint.sh /

RUN chmod +x /zombodb-entrypoint.sh

ENTRYPOINT ["/zombodb-entrypoint.sh"]

CMD ["postgres"]
