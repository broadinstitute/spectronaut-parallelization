FROM ubuntu:22.04
ENV DEBIAN_FRONTEND=noninteractive

RUN set -ex \
     && apt-get update \
     && apt-get install -y --no-install-recommends apt-transport-https ca-certificates curl gnupg \
     && curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg \
     | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg \
     && echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" \
     > /etc/apt/sources.list.d/google-cloud-sdk.list \
     && apt-get update \
     && apt-get install -y --no-install-recommends \
          dotnet-sdk-8.0 \
          google-cloud-cli \
          zip \
          clang \
          python3 \
          python3-pip \
     && rm -rf /var/lib/apt/lists/*

COPY src/Spectronaut_20.3.251119.92449.deb /packages/Spectronaut_20.3.251119.92449.deb
RUN dpkg -i /packages/Spectronaut_20.3.251119.92449.deb

RUN spectronaut activate 16994635-6f54-429c-8c4a-a2cf00781603