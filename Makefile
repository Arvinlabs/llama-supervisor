BINARY  := llama-supervisor
CONFIG  := config.yaml

.PHONY: all build run test vet fmt clean

all: build

build:
	CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o $(BINARY) .

run: build
	./$(BINARY) -config $(CONFIG)

test:
	go test ./...

vet:
	go vet ./...

fmt:
	gofmt -w -s .

clean:
	rm -f $(BINARY)
