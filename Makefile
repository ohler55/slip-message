
all: build

clean:
	rm *.so

lint:
	golangci-lint run

build:
	go build -buildmode=plugin -o message.so *.go

test: lint
	go test -coverprofile=cov.out

.PHONY: all build
