FROM --platform=$BUILDPLATFORM golang:1.27.3@sha256:f20d83a83e18dbd2a4714b8ffd4b30b287b6243962dd75608e86abcc63ba4efc AS build
ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
ARG REVISION=unknown
ARG BUILTAT=unknown
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH:-amd64} \
    go build -trimpath \
      -ldflags="-s -w \
        -X github.com/cocoonstack/vk-sandbox/version.VERSION=${VERSION} \
        -X github.com/cocoonstack/vk-sandbox/version.REVISION=${REVISION} \
        -X github.com/cocoonstack/vk-sandbox/version.BUILTAT=${BUILTAT}" \
      -o /out/vk-sandbox .

FROM gcr.io/distroless/static-debian13:nonroot@sha256:18f4cc26c9ffa7f20a359298be7deadac5c607c8e03a02b8119f1a5fc2f82c99
COPY --from=build /out/vk-sandbox /vk-sandbox

ENTRYPOINT ["/vk-sandbox"]
