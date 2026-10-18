FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/offline-scheduler ./cmd/scheduler

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/offline-scheduler /offline-scheduler
USER nonroot:nonroot
ENTRYPOINT ["/offline-scheduler"]
CMD ["-config=/etc/offline-scheduler/config.yaml"]
