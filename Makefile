#!/usr/bin/env make -f


current_dir := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))
IMAGE = allanj92/didomi-data-challenge:latest
DOCKER_RUN = docker run --rm -it -v $(current_dir):/app


build:
	docker build -t $(IMAGE) .

run:
	$(DOCKER_RUN) $(IMAGE)

dev:
	$(DOCKER_RUN) $(IMAGE) pytest-watch

coverage:
	$(DOCKER_RUN) $(IMAGE) pytest --cov=spark_jobs tests/

bash:
	$(DOCKER_RUN) $(IMAGE) bash

notebook:
	$(DOCKER_RUN) -p 4040:4040 -p 8888:8888 $(IMAGE) jupyter notebook