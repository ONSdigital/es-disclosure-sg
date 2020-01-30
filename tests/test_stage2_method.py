import unittest
from unittest import mock

import stage2_method


class MockContext:
    aws_request_id = 666


context_object = MockContext
with open('tests/fixtures/indata2and3.json') as file:
    input_data = file.read()
mock_method_event = {
                "json_data": input_data,
                "disclosivity_marker": "disclosive",
                "publishable_indicator": "publish",
                "explanation": "reason",
                "parent_column": "enterprise_reference",
                "threshold": "3",
                "reference": "responder_id",
                "total_columns": ["Q608_total"]
            }

mock_method_event_b = {
                "json_data": input_data,
                "disclosivity_marker": "disclosive",
                "publishable_indicator": "publish",
                "explanation": "reason",
                "parent_column": "enterprise_reference",
                "threshold": "3",
                "reference": "responder_id",
                "total_columns": ["Q608_total", "Q606_other_gravel"]
            }


class TestMethod(unittest.TestCase):
    def test_happy_path_mike(self):
        out = stage2_method.lambda_handler(mock_method_event, context_object)
        with open('tests/fixtures/outdata2.json') as file:
            output_data = file.read()
        assert(out['data'] == output_data)

    def test_happy_path_multiple_columns(self):
        out = stage2_method.lambda_handler(mock_method_event_b, context_object)
        with open('tests/fixtures/outdata2_b.json') as file:
            output_data = file.read()
        assert(out['data'] == output_data)

    def test_value_error(self):
        mock_method_event.pop("parent_column")
        out = stage2_method.lambda_handler(mock_method_event, context_object)
        assert(not out['success'])
        assert("Parameter validation error" in out['error'])

    def test_generic_error(self):
        with mock.patch("stage2_method.EnvironSchema") as mock_environ:
            mock_environ.side_effect = Exception()
            out = stage2_method.lambda_handler({"BadKey": "BadVal"},
                                               context_object)
            assert (not out['success'])
            assert ("General Error" in out['error'])
