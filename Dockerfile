from openjdk:8-jre-alpine3.7

RUN apk add --update bash && rm -rf /var/cache/apk/*

ADD ./metafacture-core-5.0.0-dist /app

RUN mkdir /mfwf
VOLUME /mfwf
WORKDIR /mfwf

ENTRYPOINT ["/bin/bash", "/app/metafacture-core-5.0.0-dist/flux.sh"]
