import json
import os
import unittest
from builtins import classmethod
from unittest import mock

import pandas as pd
from botocore.exceptions import ClientError, IncompleteReadError
from es_aws_functions import exception_classes

import disclosure_wrangler


class MockContext:
    aws_request_id = 666


context_object = MockContext

mock_wrangles_event = {
  "RuntimeVariables": {
    "disclosivity_marker": "disclosive",
    "publishable_indicator": "publish",
    "explanation": "reason",
    "total_column": "Q608_total",
    "parent_column": "ent_ref_count",
    "threshold": "3",
    "cell_total_column": "ent_ref_count",
    "top1_column": "largest_contributor",
    "top2_column": "second_largest_contributor",
    "stage5_threshold": "0.2",
    "disclosure_stages": "5 2 1",
    "id": "bob"
  }
}


class TestWrangler(unittest.TestCase):
    @classmethod
    def setup_class(cls):
        cls.mock_os_patcher = mock.patch.dict(
            "os.environ",
            {
                "checkpoint": "testing",
                "bucket_name": "testing",
                "in_file_name": "testing",
                "incoming_message_group": "testing",
                "method_name": "es-disclosure-stage--method",
                "out_file_name": "testing",
                "sns_topic_arn": "testing",
                "sqs_message_group_id": "testing",
                "sqs_queue_url": "testing",
                "csv_file_name": "defiantly_not_a_cake"
            }
        )

        cls.mock_os_patcher.start()

    @classmethod
    def teardown_class(cls):
        cls.mock_os_patcher.stop()

    @mock.patch('disclosure_wrangler.aws_functions.write_dataframe_to_csv')
    @mock.patch('disclosure_wrangler.aws_functions.save_data')
    @mock.patch('disclosure_wrangler.boto3.client')
    @mock.patch('disclosure_wrangler.invoke_method')
    @mock.patch('disclosure_wrangler.aws_functions.get_dataframe')
    def test_happy_path(self, mock_get_dataframe, mock_invoke_method,
                        mock_boto, mock_save_data, mock_write_csv):

        with open('tests/fixtures/factorsdata.json') as file:
            mock_get_dataframe.return_value = pd.DataFrame(json.loads(file.read())), 666

        with open('tests/fixtures/methoddata.json') as file:
            mock_invoke_method.return_value = {"data": file.read(),
                                               "success": True}

        result = disclosure_wrangler.lambda_handler(mock_wrangles_event, context_object)

        self.assertTrue(result['success'])

    @mock.patch('disclosure_wrangler.invoke_method')
    @mock.patch('disclosure_wrangler.aws_functions.get_dataframe')
    def test_method_failure(self, mock_get_dataframe, mock_invoke_method):
        with open('tests/fixtures/factorsdata.json') as file:
            mock_get_dataframe.return_value = pd.DataFrame(json.loads(file.read())), 666

        with open('tests/fixtures/methoddata.json') as file:
            mock_invoke_method.return_value = {"error": "METHOD_FAIL", "success": False}
        with unittest.TestCase.assertRaises(
                self, exception_classes.LambdaFailure) as exc_info:
            disclosure_wrangler.lambda_handler(mock_wrangles_event,
                                               context_object)
        assert "METHOD_FAIL" in exc_info.exception.error_message

    @mock.patch('disclosure_wrangler.boto3.client')
    def test_attribute_error(self, mock_client):
        mock_client.side_effect = AttributeError("Oh no!")
        with unittest.TestCase.assertRaises(
                self, exception_classes.LambdaFailure) as exc_info:
            disclosure_wrangler.lambda_handler(mock_wrangles_event, context_object)
        assert "Bad data encountered in" in exc_info.exception.error_message

    def test_value_error(self):
        os.environ.pop('bucket_name')
        with unittest.TestCase.assertRaises(
                self, exception_classes.LambdaFailure) as exc_info:
            disclosure_wrangler.lambda_handler(mock_wrangles_event, context_object)

        os.environ['bucket_name'] = "testing"
        assert "Parameter validation error" in exc_info.exception.error_message

    @mock.patch('disclosure_wrangler.boto3.client')
    def test_client_error(self, mock_client):
        mock_client.side_effect = ClientError({'Error': {'Code': '500'}}, '.')
        with unittest.TestCase.assertRaises(
                self, exception_classes.LambdaFailure) as exc_info:
            disclosure_wrangler.lambda_handler(mock_wrangles_event, context_object)
        assert "AWS Error" in exc_info.exception.error_message

    @mock.patch('disclosure_wrangler.boto3.client')
    def test_key_error(self, mock_client):
        mock_client.side_effect = KeyError("Oh no!")
        with unittest.TestCase.assertRaises(
                self, exception_classes.LambdaFailure) as exc_info:
            disclosure_wrangler.lambda_handler(mock_wrangles_event, context_object)
            disclosure_wrangler.lambda_handler(mock_wrangles_event, context_object)
        assert "Key Error in" in exc_info.exception.error_message

    @mock.patch('disclosure_wrangler.boto3.client')
    def test_lambda_error(self, mock_client):
        mock_client.side_effect = IncompleteReadError(actual_bytes=0, expected_bytes=99)
        with unittest.TestCase.assertRaises(
                self, exception_classes.LambdaFailure) as exc_info:
            disclosure_wrangler.lambda_handler(mock_wrangles_event, context_object)
        assert "Incomplete Lambda response encountered in " in \
               exc_info.exception.error_message

    @mock.patch('disclosure_wrangler.boto3.client')
    def test_exception_error(self, mock_client):
        mock_client.side_effect = Exception("Oh no!")
        with unittest.TestCase.assertRaises(
                self, exception_classes.LambdaFailure) as exc_info:
            disclosure_wrangler.lambda_handler(mock_wrangles_event, context_object)
        assert "General Error in" in exc_info.exception.error_message
