FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/api ./cmd/api \
    && CGO_ENABLED=0 go build -trimpath -o /out/load ./cmd/load \
    && CGO_ENABLED=0 go build -trimpath -o /out/bench ./cmd/bench

FROM alpine:3.21
RUN apk add --no-cache ca-certificates \
    && adduser -D -H -u 65532 app \
    && mkdir -p /app
COPY --from=build /out/api /out/load /out/bench /app/
USER app
EXPOSE 8080
ENTRYPOINT ["/app/api"]
