
all: build

clean:
	rm -f *.so

lint:
	golangci-lint run

build:
	go mod tidy
	go build -buildmode=plugin -o message.so *.go

test: lint
	go test -coverprofile=cov.out ./...

.PHONY: all build
