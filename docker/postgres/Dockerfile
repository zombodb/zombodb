#
# Postgres with ZomboDB
#

# Pull base image.
FROM postgres:9.5
ENV ZOMBODB_VER 3.1.0_BETA2

# Fetch wget
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates wget && rm -rf /var/lib/apt/lists/*
RUN apt-get update -y -qq --fix-missing
RUN apt-get install -y curl

# Install zombo
RUN \
  cd / && \
  mkdir zombodb && \
  cd zombodb && \
  wget https://github.com/zombodb/zombodb/releases/download/v${ZOMBODB_VER}/zombodb_jessie_pg95-${ZOMBODB_VER}_amd64.deb

RUN \
  cd /zombodb && \
  dpkg -i zombodb_jessie_pg95-${ZOMBODB_VER}_amd64.deb

COPY zombodb-entrypoint.sh /

RUN chmod +x /zombodb-entrypoint.sh

ENTRYPOINT ["/zombodb-entrypoint.sh"]

CMD ["postgres"]
