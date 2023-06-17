FROM golang:alpine3.18 as build
WORKDIR /go/src/app
COPY . .
RUN mkdir /go/bin/app
RUN go build -o /go/bin/app/ -v ./cmd/mercurius

FROM alpine:3.18
COPY --from=build /go/bin/app /app
ENTRYPOINT ["/app/mercurius"]