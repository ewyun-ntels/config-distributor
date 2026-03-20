# syntax=docker/dockerfile:1

##############
# Build Stage
##############
FROM golang:1.24-alpine AS builder

RUN apk add --no-cache ca-certificates make git bash

WORKDIR /workspace
COPY . .

RUN make distributor

ENV OUTDIR=/out
RUN mkdir -p ${OUTDIR}/usr/local/bin/
RUN mv /workspace/bin/cfg-distributor ${OUTDIR}/usr/local/bin/

##############
# Final Stage
##############
FROM alpine:3.22.2

RUN apk add --no-cache ca-certificates

COPY --from=builder /out/ /

WORKDIR /
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/cfg-distributor"]
