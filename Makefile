.PHONY: up down topics bootstrap produce-stock produce-iot stream alert-consumer dashboard test

up:
	docker compose up -d

down:
	docker compose down

topics:
	docker exec -it pipeline-kafka bash -c "$$(cat infra/create_topics.sh)"

bootstrap:
	python -m aws.bootstrap_aws

produce-stock:
	python -m producer.stock_producer --rate 20

produce-iot:
	python -m producer.iot_producer --rate 20

stream:
	spark-submit \
		--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
		streaming/spark_streaming_job.py

alert-consumer:
	python -m consumer.alert_consumer

dashboard:
	streamlit run dashboard/app.py

test:
	pytest tests/ -v
