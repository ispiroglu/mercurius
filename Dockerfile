FROM golang:alpine

WORKDIR /go/src/app

COPY . /go/src/app

RUN go build -o /go/bin/app -v ./...

ENTRYPOINT ["/go/bin/app"]