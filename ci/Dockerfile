ARG ALPINE=alpine:3.24.2

FROM $ALPINE

RUN apk --no-cache --no-progress add tzdata ca-certificates

COPY kafka-console /
ENTRYPOINT [ "/kafka-console" ]
