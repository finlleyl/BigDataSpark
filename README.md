```bash
docker-compose up -d --build
```

```bash
docker exec -it spark-master spark-submit \
  --master spark://spark-master:7077 \
  --jars /opt/spark/jars-extra/postgresql-42.7.3.jar \
  /opt/spark-jobs/job1_star_schema.py
```

```bash
docker exec -it spark-master spark-submit \
  --master spark://spark-master:7077 \
  --jars /opt/spark/jars-extra/postgresql-42.7.3.jar,/opt/spark/jars-extra/clickhouse-jdbc-0.6.0-all.jar \
  /opt/spark-jobs/job2_reports_clickhouse.py
```

```bash
docker exec -it spark-lab-clickhouse clickhouse-client
```
