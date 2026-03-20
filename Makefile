SHELL := /bin/bash

GO ?= go
BIN_DIR := bin
DIST_BIN := $(BIN_DIR)/cfg-distributor
DIST_PKG := ./cmd/distributor
IMAGE ?= cfg-distributor
DOCKERFILE ?= build/distributor/Dockerfile

.PHONY: all distributor clean test run tidy container

all: distributor

distributor: $(DIST_BIN)

$(DIST_BIN):
	@mkdir -p $(BIN_DIR)
	$(GO) build -o $(DIST_BIN) $(DIST_PKG)

run:
	$(GO) run $(DIST_PKG)

test:
	$(GO) test ./...

tidy:
	$(GO) mod tidy

container:
	docker build -f $(DOCKERFILE) -t $(IMAGE) .

clean:
	rm -rf $(BIN_DIR)
