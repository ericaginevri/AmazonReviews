from flask import Flask, jsonify, render_template
from flask_cors import CORS
import subprocess

app = Flask(__name__)
CORS(app)


@app.route("/")
def home():
    return render_template("index.html")


@app.route("/jobs/hadoop", methods=["POST"])
def run_hadoop():

    return jsonify({
        "status": "OK",
        "message": "Hadoop job started"
    })


@app.route("/jobs/spark", methods=["POST"])
def run_spark():

    return jsonify({
        "status": "OK",
        "message": "Spark job started"
    })


@app.route("/results/hadoop")
def get_hadoop_results():

    return jsonify({
        "results": "No results yet"
    })


@app.route("/results/spark")
def get_spark_results():

    return jsonify({
        "results": "No results yet"
    })


if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=8080
    )
