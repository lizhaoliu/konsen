FROM golang:1.24-alpine

WORKDIR /konsen

COPY . ./

RUN go mod download && \
    go build -o ./konsen ./cmd/main.go

CMD ["/konsen/konsen"]