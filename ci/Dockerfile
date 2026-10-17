ARG ALPINE=alpine:3.24.2

FROM $ALPINE

RUN apk --no-cache --no-progress add tzdata ca-certificates

# GoReleaser puts each platform's binary under linux/<arch>/ in the context.
ARG TARGETPLATFORM
COPY $TARGETPLATFORM/kafka-console /
ENTRYPOINT [ "/kafka-console" ]
