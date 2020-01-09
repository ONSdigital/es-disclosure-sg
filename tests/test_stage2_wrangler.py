import json
import unittest
from unittest import mock

import boto3
import pandas as pd
from botocore.exceptions import IncompleteReadError
from moto import mock_lambda, mock_sqs

import stage2_wrangler


class MockContext:
    aws_request_id = 666


context_object = MockContext
mock_wrangles_event = {
  "RuntimeVariables": {
    "disclosivity_marker": "disclosive",
    "publishable_indicator": "publish",
    "explanation": "reason",
    "parent_column": "enterprise_reference",
    "threshold": "3"
  }
}


class TestWrangler(unittest.TestCase):

    def test_environment_variable_exception(self):
        with mock.patch.dict('os.environ', {
            'checkpoint': 'mock_checkpoint',
            'sns_topic_arn': 'not_an_arn',
            'file_name': 'mock_method',
            'bucket_name': 'Bertie Bucket'

        }):

            out = stage2_wrangler.lambda_handler(mock_wrangles_event,
                                                 context_object)
            assert(not out['success'])
            assert ("Parameter validation error" in out['error'])

    def test_key_error(self):
        with mock.patch.dict('os.environ', {
            'sns_topic_arn': 'not_an_arn',
            'method_name': 'mock_method',
            'sqs_message_group_id': 'sqsmessid',
            'sqs_queue_url': "Q",
            'incoming_message_group': "Innnnncoooommming",
            'in_file_name': "Axel Filey",
            'out_file_name': "Filey McGee",
            'bucket_name': 'Mrs Bucket',
            'checkpoint': '0'
        }):

            out = stage2_wrangler.lambda_handler({
                  "Jam": {}}, context_object)
            assert(not out['success'])
            assert ("Key Error" in out['error'])

    @mock_lambda
    @mock_sqs
    def test_happy_path(self):
        sqs = boto3.resource("sqs", region_name="eu-west-2")
        sqs.create_queue(QueueName="test_queue")
        queue_url = sqs.get_queue_by_name(QueueName="test_queue").url
        with mock.patch.dict('os.environ', {
            'sns_topic_arn': 'not_an_arn',
            'method_name': 'mock_method',
            'sqs_message_group_id': 'sqsmessid',
            'sqs_queue_url': queue_url,
            'incoming_message_group': "Innnnncoooommming",
            'in_file_name': "Axel Filey",
            'out_file_name': "Filey McGee",
            'bucket_name': 'Mrs Bucket',
            'checkpoint': '0'
        }):
            with open('tests/fixtures/factorsdata.json') as file:
                s3_data = file.read()
            with open('tests/fixtures/test_s3_data.json') as file:
                lambda_return = file.read()
            with mock.patch("stage2_wrangler.aws_functions") as mock_funk:
                with mock.patch('stage2_wrangler.boto3.client') as mock_boto:
                    mocked_client = mock.Mock()
                    mock_boto.return_value = mocked_client
                    mocked_client.invoke.\
                        return_value.get.\
                        return_value.read.\
                        return_value.decode.\
                        return_value = \
                        json.dumps({"data": lambda_return, "success": True})
                    mock_funk.get_dataframe.return_value \
                        = pd.DataFrame(json.loads(s3_data)), 666
                    out = stage2_wrangler.lambda_handler(mock_wrangles_event,
                                                         context_object)
                    assert(out['success'])

    @mock_lambda
    @mock_sqs
    def test_attribute_error(self):
        sqs = boto3.resource("sqs", region_name="eu-west-2")
        sqs.create_queue(QueueName="test_queue")
        queue_url = sqs.get_queue_by_name(QueueName="test_queue").url
        with mock.patch.dict('os.environ', {
            'sns_topic_arn': 'not_an_arn',
            'method_name': 'mock_method',
            'sqs_message_group_id': 'sqsmessid',
            'sqs_queue_url': queue_url,
            'incoming_message_group': "Innnnncoooommming",
            'in_file_name': "Axel Filey",
            'out_file_name': "Filey McGee",
            'bucket_name': 'Mrs Bucket',
            'checkpoint': '0'
        }):
            with mock.patch("stage2_wrangler.aws_functions.get_dataframe") as mock_sqs:
                with mock.patch('stage1_wrangler.boto3.client') as mock_boto:
                    mock_boto.side_effect = AttributeError("Oh no!")
                    mock_sqs.return_value = {""}
                    out = stage2_wrangler.lambda_handler(mock_wrangles_event,
                                                         context_object)
                    assert (not out['success'])
                    assert("Bad data encountered in" in out['error'])

    @mock_sqs
    def test_client_error(self):
        with mock.patch.dict('os.environ', {
            'sns_topic_arn': 'not_an_arn',
            'method_name': 'mock_method',
            'sqs_message_group_id': 'sqsmessid',
            'sqs_queue_url': "Q",
            'incoming_message_group': "Innnnncoooommming",
            'in_file_name': "Axel Filey",
            'out_file_name': "Filey McGee",
            'bucket_name': 'Mrs Bucket',
            'checkpoint': '0'
        }):
            mock_sqs.return_value = {"NoAMessages": "NotData"}
            out = stage2_wrangler.lambda_handler(mock_wrangles_event,
                                                 context_object)
            assert (not out['success'])
            assert("AWS Error" in out['error'])

    @mock_lambda
    @mock_sqs
    def test_incomplete_read_error(self):
        sqs = boto3.resource("sqs", region_name="eu-west-2")
        sqs.create_queue(QueueName="test_queue")
        queue_url = sqs.get_queue_by_name(QueueName="test_queue").url
        with mock.patch.dict('os.environ', {
            'sns_topic_arn': 'not_an_arn',
            'method_name': 'mock_method',
            'sqs_message_group_id': 'sqsmessid',
            'sqs_queue_url': queue_url,
            'incoming_message_group': "Innnnncoooommming",
            'in_file_name': "Axel Filey",
            'out_file_name': "Filey McGee",
            'bucket_name': 'Mrs Bucket',
            'checkpoint': '0'
        }):
            with mock.patch("stage2_wrangler.aws_functions.get_dataframe") as mock_sqs:
                with mock.patch('stage2_wrangler.boto3.client') as mock_boto:
                    mock_boto.side_effect = \
                        IncompleteReadError(actual_bytes=0, expected_bytes=99)
                    mock_sqs.return_value = {""}
                    out = stage2_wrangler.lambda_handler(mock_wrangles_event,
                                                         context_object)

                    assert (not out['success'])
                    assert ("Incomplete Lambda response encountered" in out['error'])

    @mock_lambda
    @mock_sqs
    def test_generic_error(self):
        sqs = boto3.resource("sqs", region_name="eu-west-2")
        sqs.create_queue(QueueName="test_queue")
        queue_url = sqs.get_queue_by_name(QueueName="test_queue").url
        with mock.patch.dict('os.environ', {
            'sns_topic_arn': 'not_an_arn',
            'method_name': 'mock_method',
            'sqs_message_group_id': 'sqsmessid',
            'sqs_queue_url': queue_url,
            'incoming_message_group': "Innnnncoooommming",
            'in_file_name': "Axel Filey",
            'out_file_name': "Filey McGee",
            'bucket_name': 'Mrs Bucket',
            'checkpoint': '0'
        }):
            with mock.patch('stage2_wrangler.boto3.client') as mock_boto:
                mock_boto.side_effect = Exception()
                mock_sqs.return_value = {""}
                out = stage2_wrangler.lambda_handler(mock_wrangles_event,
                                                     context_object)
                assert (not out['success'])
                assert ("General Error" in out['error'])

    @mock_lambda
    @mock_sqs
    def test_method_error(self):
        sqs = boto3.resource("sqs", region_name="eu-west-2")
        sqs.create_queue(QueueName="test_queue")
        queue_url = sqs.get_queue_by_name(QueueName="test_queue").url
        with mock.patch.dict('os.environ', {
            'sns_topic_arn': 'not_an_arn',
            'method_name': 'mock_method',
            'sqs_message_group_id': 'sqsmessid',
            'sqs_queue_url': queue_url,
            'incoming_message_group': "Innnnncoooommming",
            'in_file_name': "Axel Filey",
            'out_file_name': "Filey McGee",
            'bucket_name': 'Mrs Bucket',
            'checkpoint': '0'
        }):
            with open('tests/fixtures/factorsdata.json') as file:
                s3_data = file.read()
            with mock.patch("stage2_wrangler.aws_functions.get_dataframe") as mock_funk:
                with mock.patch('stage2_wrangler.boto3.client') as mock_boto:
                    mock_boto.return_value.invoke.return_value.get.return_value \
                        .read.return_value.decode.return_value = \
                        json.dumps({"error": "This is an error message",
                                    "success": False})
                    mock_funk.return_value \
                        = pd.DataFrame(json.loads(s3_data)), 666
                    out = stage2_wrangler.lambda_handler(mock_wrangles_event,
                                                         context_object)

                    assert "success" in out
                    assert out["success"] is False
                    assert out["error"].__contains__("""This is an error message""")
