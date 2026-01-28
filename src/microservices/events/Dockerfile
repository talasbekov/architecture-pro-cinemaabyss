FROM golang:1.25-alpine AS builder

WORKDIR /app

# Install git for fetching dependencies
RUN apk add --no-cache git

COPY go.mod ./
RUN go mod download || true

COPY . .

# Download dependencies and build
RUN go mod tidy && CGO_ENABLED=0 GOOS=linux go build -o events .

FROM alpine:latest

WORKDIR /app

COPY --from=builder /app/events .

EXPOSE 8082

CMD ["./events"]
