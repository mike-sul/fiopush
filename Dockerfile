FROM golang:alpine as builder

RUN apk update apk add --no-cache git

RUN mkdir /build
COPY . /build/
WORKDIR /build
RUN go build -o ostreehub main.go

FROM alpine
COPY --from=builder /build/ostreehub /usr/local/bin/ostreehub
