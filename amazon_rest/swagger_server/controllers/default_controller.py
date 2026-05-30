import connexion
import six
from flask import jsonify

from swagger_server.models.hadoop_request import HadoopRequest  # noqa: E501
from swagger_server.models.spark_request import SparkRequest  # noqa: E501
from swagger_server import util


def get_hadoop_results():  # noqa: E501
    """Get Hadoop results

     # noqa: E501


    :rtype: None
    """
    return 'do some magic!'


def get_spark_results():  # noqa: E501
    """Get Spark results

     # noqa: E501


    :rtype: None
    """
    return 'do some magic!'


def run_hadoop(body):

    if connexion.request.is_json:

        body = HadoopRequest.from_dict(
            connexion.request.get_json()
        )

        print("DATASET:", body.dataset)

        print("JOB1 REDUCERS:", body.job1_reducers)
        print("JOB2 REDUCERS:", body.job2_reducers)
        print("JOB3 REDUCERS:", body.job3_reducers)

        return jsonify(
            dataset=body.dataset,
            job1Reducers=body.job1_reducers,
            job2Reducers=body.job2_reducers,
            job3Reducers=body.job3_reducers
        )

    return "Invalid request", 400

def run_spark(body):

    if connexion.request.is_json:

        body = SparkRequest.from_dict(
            connexion.request.get_json()
        )

        print("DATASET:", body.dataset)

        print("JOB1 PARTITIONS:", body.job1_partitions)
        print("JOB2 PARTITIONS:", body.job2_partitions)
        print("JOB3 PARTITIONS:", body.job3_partitions)

        return jsonify(
            dataset=body.dataset,
            job1Partitions=body.job1_partitions,
            job2Partitions=body.job2_partitions,
            job3Partitions=body.job3_partitions
        )

    return "Invalid request", 400
