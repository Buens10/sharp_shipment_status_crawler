.PHONY: build
# target: build – build the docker image
build:
	docker-compose -f docker-compose.yml build

.PHONY: up
# target: up – start containers
up:
	docker-compose -f docker-compose.yml up
