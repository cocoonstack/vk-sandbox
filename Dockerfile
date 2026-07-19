FROM golang:1.26 AS build
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/vk-cocoon-sandbox .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/vk-cocoon-sandbox /vk-cocoon-sandbox
ENTRYPOINT ["/vk-cocoon-sandbox"]
