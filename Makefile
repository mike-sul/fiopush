.PHONY: dir

bd="bin"
exe="ostreehub"
push_exe="fiopush"
linter:=$(shell which golangci-lint 2>/dev/null || echo $(HOME)/go/bin/golangci-lint)

all: $(exe) $(push_exe)

$(bd):
	@mkdir -p $@

$(exe): $(bd) main.go
	go build -o $(bd)/$(exe) main.go

$(push_exe): $(bd) cmd/fiopush/main.go
	go build -o $(bd)/$(push_exe) cmd/fiopush/main.go

clean:
	@rm -r $(bd)

format:
	@gofmt -l -w ./

check:
	@test -z $(shell gofmt -l ./) || echo "[WARN] Fix formatting issues with 'make fmt'"
	$(linter) run

container:
	docker build -t ostreehub-dev .


run:
	docker run -it --rm -p 9101:9101 -e GOOGLE_APPLICATION_CREDENTIALS=/secrets/gce-key.json -v ${GOOGLE_APPLICATION_CREDENTIALS}:/secrets/gce-key.json  -t ostreehub-dev /usr/local/bin/ostreehub
