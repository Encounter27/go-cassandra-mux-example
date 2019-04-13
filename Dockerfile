# build stage
FROM golang:alpine
#AS builder

RUN apk update && apk add git

WORKDIR /go

RUN mkdir -p /go/pkg
RUN mkdir -p /go/bin
RUN mkdir -p /go/src/app

COPY main.go /go/src/app/

RUN apk add --no-cache git

# Download dependencies
RUN go get -d -v ./src/app/...

# Build the app
RUN go install -v ./src/app/...

RUN ls ./bin/

ENTRYPOINT ./bin/app
LABEL Name=go-cassandra Version=0.0.1
EXPOSE 8080
