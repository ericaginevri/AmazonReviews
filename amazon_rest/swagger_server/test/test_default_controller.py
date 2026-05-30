# coding: utf-8

from __future__ import absolute_import

from flask import json
from six import BytesIO

from swagger_server.models.hadoop_request import HadoopRequest  # noqa: E501
from swagger_server.models.spark_request import SparkRequest  # noqa: E501
from swagger_server.test import BaseTestCase


class TestDefaultController(BaseTestCase):
    """DefaultController integration test stubs"""

    def test_get_hadoop_results(self):
        """Test case for get_hadoop_results

        Get Hadoop results
        """
        response = self.client.open(
            '/v1/results/hadoop',
            method='GET')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_get_spark_results(self):
        """Test case for get_spark_results

        Get Spark results
        """
        response = self.client.open(
            '/v1/results/spark',
            method='GET')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_run_hadoop(self):
        """Test case for run_hadoop

        Run Hadoop job
        """
        body = HadoopRequest()
        response = self.client.open(
            '/v1/hadoop',
            method='POST',
            data=json.dumps(body),
            content_type='application/json')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_run_spark(self):
        """Test case for run_spark

        Run Spark job
        """
        body = SparkRequest()
        response = self.client.open(
            '/v1/spark',
            method='POST',
            data=json.dumps(body),
            content_type='application/json')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))


if __name__ == '__main__':
    import unittest
    unittest.main()
