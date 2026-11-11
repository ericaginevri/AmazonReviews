# Amazon Reviews
*Cloud Computing Project — Academic Year 2025/2026*

A big-data pipeline that analyses Amazon product reviews with **Hadoop MapReduce** and **Apache Spark**, and serves the results through a **REST API**.

**Team (MirandaPriestley Group):** Erica Ginevri, Chiara Nasoni, Vittoria Vellutini

## 🔎 What it does

The pipeline is made of three chained MapReduce jobs:

1. **Aggregation**: groups the reviews by product and computes review count, average rating, verified reviews and helpful votes.
2. **Ranking**: gives every product a score that balances rating, review volume and helpfulness, `avg_rating × log(1 + reviews) × (1 + helpful / (1 + total votes))`, and ranks the products within each category.
3. **Anomaly detection**: flags products whose score is not backed by the evidence, for example a high score with few helpful votes or few verified purchases.

The same pipeline is also implemented in Spark, to compare the two approaches. The full analysis is in [`REPORT.pdf`](REPORT.pdf).

## 📁 Project structure

```
amazon_project/            Hadoop MapReduce jobs (Java, Maven)
spark/amazon_spark.py      Spark version of the pipeline (PySpark)
amazon_rest/               REST server and web client (Python, Docker)
non_parallel_execution.py  Script that manages the execution of the Hadoop jobs
REPORT.pdf                 Project report
```

## 🚀 Getting started

The jobs ran on the course's Hadoop cluster, reachable through the university VPN. To start the REST server:

```bash
cd amazon_rest
docker build -t rest-server .
docker run -p 8080:8080 rest-server
```

Then open `http://<host>:8080` in your browser to explore the results.
