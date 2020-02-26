#!/usr/bin/env make -f

IMAGE = allanj92/didomi-data-challenge:latest

current_dir := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))


build:
	docker build -f .docker/Dockerfile -t $(IMAGE) .
dev:
	docker run -it -v $(current_dir):/app $(IMAGE)