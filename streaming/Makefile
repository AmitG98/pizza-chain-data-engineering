COMPOSE_FILE=docker-compose.yml

# Start only Kafka & Zookeeper
kafka:
	docker compose -f $(COMPOSE_FILE) up -d zookeeper kafka

# Build all producers
build-producers:
	docker compose -f $(COMPOSE_FILE) build order-producer weather-producer late-complaint-producer

# Build all consumers
build-consumers:
	docker compose -f $(COMPOSE_FILE) build order-consumer weather-consumer late-complaint-consumer

# Build everything
build:
	docker compose -f $(COMPOSE_FILE) build

# Run all producers
run-producers:
	docker compose -f $(COMPOSE_FILE) up -d order-producer weather-producer late-complaint-producer

# Run all consumers
run-consumers:
	docker compose -f $(COMPOSE_FILE) up -d order-consumer weather-consumer late-complaint-consumer

# Run full pipeline: Kafka + Producers + Consumers
run-all:
	docker compose down -v
	docker compose build
	docker compose up -d

# Run a specific producer
run-order-producer:
	docker compose -f $(COMPOSE_FILE) up order-producer

run-weather-producer:
	docker compose -f $(COMPOSE_FILE) up weather-producer

run-late-producer:
	docker compose -f $(COMPOSE_FILE) up late-complaint-producer

# Run a specific consumer
run-order-consumer:
	docker compose -f $(COMPOSE_FILE) up order-consumer

run-weather-consumer:
	docker compose -f $(COMPOSE_FILE) up weather-consumer

run-late-consumer:
	docker compose -f $(COMPOSE_FILE) up late-complaint-consumer

# Shutdown all services
down:
	docker compose -f $(COMPOSE_FILE) down
