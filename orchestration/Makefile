# Start Airflow services
airflow-up:
	docker compose up -d airflow-webserver airflow-scheduler

# Stop all Airflow containers
airflow-down:
	docker compose down

# View logs
airflow-logs:
	docker compose logs -f

# Restart services
airflow-restart: airflow-down airflow-up

# Initialize Airflow DB (only for first time)
airflow-init:
	docker compose run --rm airflow-webserver airflow db init

# Open bash in webserver
airflow-bash:
	docker compose exec airflow-webserver bash
