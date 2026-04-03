FROM golang:1.24-alpine AS builder

ENV CGO_ENABLED=0 \
    GOOS=linux \
    GOARCH=amd64

WORKDIR /app

COPY . ./
RUN go build -o app

FROM scratch

# Run as non-root user. Docker article here: https://www.docker.com/blog/understanding-the-docker-user-instruction/
USER 1001
COPY --from=builder /app/app /app
EXPOSE 8080

ENTRYPOINT ["./app"]
