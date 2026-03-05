# Build stage
FROM golang:1.24.13-alpine3.22 AS builder

WORKDIR /app

# Install dependencies
RUN apk add --no-cache git

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build all binaries
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/shard ./cmd/shard
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/mulberry ./cmd/mulberry
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/csrs ./cmd/csrs
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/client ./cmd/client

# Shard image
FROM alpine:3.22 AS shard
RUN apk add --no-cache ca-certificates
COPY --from=builder /bin/shard /usr/local/bin/shard
ENTRYPOINT ["shard"]

# Mulberry image
FROM alpine:3.22 AS mulberry
RUN apk add --no-cache ca-certificates
COPY --from=builder /bin/mulberry /usr/local/bin/mulberry
ENTRYPOINT ["mulberry"]

# CSRS image
FROM alpine:3.22 AS csrs
RUN apk add --no-cache ca-certificates
COPY --from=builder /bin/csrs /usr/local/bin/csrs
ENTRYPOINT ["csrs"]

# Client image
FROM alpine:3.22 AS client
RUN apk add --no-cache ca-certificates
COPY --from=builder /bin/client /usr/local/bin/client
ENTRYPOINT ["client"]

# All-in-one image (default)
FROM alpine:3.22
RUN apk add --no-cache ca-certificates
COPY --from=builder /bin/shard /usr/local/bin/shard
COPY --from=builder /bin/mulberry /usr/local/bin/mulberry
COPY --from=builder /bin/csrs /usr/local/bin/csrs
COPY --from=builder /bin/client /usr/local/bin/client
