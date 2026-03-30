up:
	docker compose up --build

down:
	docker compose down

reset:
	docker compose down -v

test:
	docker compose exec airflow-api-server pytest /opt/airflow/tests

logs:
	docker compose logs -f