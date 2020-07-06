DOCKER_IMAGE_NAME=revenue-spark
CURRENT_DIR = $(shell pwd)

.PHONY: default help
default help:
	@echo 'This Makefile has the following targets:'
	@grep -o  -E '^[a-zA-Z0-9\-]+( [a-zA-Z0-9\-]+)?:( |$$)' Makefile | sed -E 's/: ?//g' | tr ' ' '\n'

.PHONY: docker-build
docker-build: 
	@echo '..building $(DOCKER_IMAGE_NAME) image..'
	docker build --compress --tag $(DOCKER_IMAGE_NAME) .

.PHONY: docker-run
docker-run: 
	@echo '..starting docker container..'
	docker run --interactive --tty --volume `pwd`/src:/src \
		--volume $(CURRENT_DIR)/data/:/root/data \
	    $(DOCKER_IMAGE_NAME) bash || true

.PHONY: docker-run-job
docker-run-job:
	@echo '..starting docker container..'
	docker run --interactive --tty --volume `pwd`/src:/src \
		--volume $(CURRENT_DIR)/data/:/root/data \
		$(DOCKER_IMAGE_NAME) bash '-c' "cd /home && export PYTHONIOENCODING=utf8 && spark-submit \
		/src/revenue.py \
		--partitions-output "4" \
		1> >(sed $$'s,.*,\e[32m&\e[m,' >&2)" || true

.PHONY: docker-pytest
docker-pytest:
	@echo '..starting docker container..'
	docker run --interactive --tty --volume `pwd`/src:/src --volume `pwd`/test:/test \
	    $(DOCKER_IMAGE_NAME) bash '-c' 'python -m pytest /test -vvv'