FROM golang:1.17.7
LABEL org.opencontainers.image.source https://github.com/harmanpa/patchworkagent
COPY . /go/patchworkagent
RUN cd /go/patchworkagent && go build
