FROM golang:1.14

COPY go.mod go.sum ./
RUN go mod download
