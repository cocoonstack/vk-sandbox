FROM golang:1.27.1@sha256:512690a5660563b57d37ecc31129e7f136e831db2aed24a1dbeb8ad7380dc0fa AS build
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/vk-sandbox .

FROM gcr.io/distroless/static-debian13:nonroot@sha256:1c2c046bc09ed40fad370b599a0b1ae7987f55b01e247cf27a7c27cd97e5bbc7
COPY --from=build /out/vk-sandbox /vk-sandbox
ENTRYPOINT ["/vk-sandbox"]
