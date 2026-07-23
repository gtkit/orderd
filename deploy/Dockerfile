# 多阶段构建。前置:CI/本地需先 `cd proto && buf generate` 生成 pb/ 再构建。
FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /orderd ./cmd/server

# 运行镜像:distroless 非 root,最小攻击面。
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /orderd /orderd
EXPOSE 9090 8080
ENTRYPOINT ["/orderd"]
