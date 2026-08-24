BINARY    := llama-supervisor
GIT_VERSION = $(shell git rev-parse --short HEAD)
VERSION = $(shell git describe --tags --always --dirty 2>/dev/null | sed 's/-g\([0-9a-f]\)/-\1/')
ifeq ($(strip $(VERSION)),)
VERSION = dev-$(GIT_VERSION)
endif
BUILD_TIME = $(shell date -u '+%Y-%m-%d_%H:%M:%S_%Z')
LDFLAGS = -s -w -X main.Version=$(VERSION) -X main.BuildTime=$(BUILD_TIME)

.DEFAULT_GOAL := build

.PHONY: build
build:
	CGO_ENABLED=0 go build -v -trimpath -ldflags "$(LDFLAGS)" -o dist/$(BINARY) .

.PHONY: build-slim
build-slim:
	CGO_ENABLED=0 go build -v -trimpath -ldflags "-s -w" -o $(BINARY) .

.PHONY: run
run: build-slim
	./$(BINARY) -config config.yaml

.PHONY: build-all
build-all: clean
	@echo "Building for all platforms..."
	@mkdir -p dist
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -v -trimpath -ldflags "$(LDFLAGS)" -o dist/$(BINARY)-linux-amd64 .
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -v -trimpath -ldflags "$(LDFLAGS)" -o dist/$(BINARY)-linux-arm64 .
	CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 go build -v -trimpath -ldflags "$(LDFLAGS)" -o dist/$(BINARY)-darwin-amd64 .
	CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go build -v -trimpath -ldflags "$(LDFLAGS)" -o dist/$(BINARY)-darwin-arm64 .
	@echo "Build complete. Binaries are in dist/"

.PHONY: build-linux
build-linux: clean
	@echo "Building for Linux..."
	@mkdir -p dist
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -v -trimpath -ldflags "$(LDFLAGS)" -o dist/$(BINARY)-linux-amd64 .
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -v -trimpath -ldflags "$(LDFLAGS)" -o dist/$(BINARY)-linux-arm64 .

.PHONY: build-darwin
build-darwin: clean
	@echo "Building for macOS..."
	@mkdir -p dist
	CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 go build -v -trimpath -ldflags "$(LDFLAGS)" -o dist/$(BINARY)-darwin-amd64 .
	CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go build -v -trimpath -ldflags "$(LDFLAGS)" -o dist/$(BINARY)-darwin-arm64 .

.PHONY: release
release: build-all
	@echo "Creating release archives..."
	cd dist && tar -czf $(BINARY)-linux-amd64.tar.gz $(BINARY)-linux-amd64
	cd dist && tar -czf $(BINARY)-linux-arm64.tar.gz $(BINARY)-linux-arm64
	cd dist && tar -czf $(BINARY)-darwin-amd64.tar.gz $(BINARY)-darwin-amd64
	cd dist && tar -czf $(BINARY)-darwin-arm64.tar.gz $(BINARY)-darwin-arm64
	@echo "Release archives created in dist/"

.PHONY: test
test:
	go test -race ./...

.PHONY: vet
vet:
	go vet ./...

.PHONY: fmt
fmt:
	gofmt -w -s .

.PHONY: install
install: build-slim
	cp $(BINARY) /usr/local/bin/$(BINARY)

.PHONY: clean
clean:
	rm -rf dist
