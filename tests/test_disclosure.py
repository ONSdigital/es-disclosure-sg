import json
from unittest import mock

import pandas as pd
import pytest
from es_aws_functions import exception_classes, test_generic_library
from moto import mock_s3
from pandas.testing import assert_frame_equal

import disclosure_wrangler as lambda_wrangler_function
import stage1_method as lambda_method_function_1
import stage2_method as lambda_method_function_2
import stage5_method as lambda_method_function_5

wrangler_environment_variables = {
    "bucket_name": "test_bucket",
    "method_name": "es-disclosure-stage--method"
}

method_runtime_variables_1 = {
    "RuntimeVariables": {
        "bpm_queue_url": "fake_queue_url",
        "cell_total_column": "cell_total",
        "data": None,
        "disclosivity_marker": "disclosive",
        "environment": "sandbox",
        "explanation": "reason",
        "publishable_indicator": "publish",
        "run_id": "666",
        "survey": "BMI_SG",
        "total_columns": ["Q608_total", "Q606_other_gravel"],
        "unique_identifier": ["responder_id"],
    }
}

method_runtime_variables_2 = {
    "RuntimeVariables": {
        "bpm_queue_url": "fake_queue_url",
        "data": None,
        "disclosivity_marker": "disclosive",
        "environment": "sandbox",
        "explanation": "reason",
        "parent_column": "ent_ref_count",
        "publishable_indicator": "publish",
        "run_id": "666",
        "survey": "BMI_SG",
        "threshold": "7",
        "total_columns": ["Q608_total", "Q606_other_gravel"],
        "unique_identifier": ["responder_id"]
    }
}

method_runtime_variables_5 = {
    "RuntimeVariables": {
        "bpm_queue_url": "fake_queue_url",
        "cell_total_column": "cell_total",
        "data": None,
        "disclosivity_marker": "disclosive",
        "environment": "sandbox",
        "explanation": "reason",
        "publishable_indicator": "publish",
        "run_id": "666",
        "survey": "BMI_SG",
        "threshold": "1",
        "top1_column": "largest_contributor",
        "top2_column": "second_largest_contributor",
        "total_columns": ["Q608_total", "Q606_other_gravel"],
        "unique_identifier": ["responder_id"]
    }
}

wrangler_runtime_variables = {
    "RuntimeVariables":
        {
            "bpm_queue_url": "fake_queue_url",
            "cell_total_column": "cell_total",
            "disclosivity_marker": "disclosive",
            "disclosure_stages": "1 2 5",
            "environment": "sandbox",
            "explanation": "reason",
            "final_output_location": "fixtures/",
            "in_file_name": "test_wrangler_input",
            "out_file_name": "test_wrangler_output.json",
            "parent_column": "ent_ref_count",
            "publishable_indicator": "publish",
            "run_id": "666",
            "sns_topic_arn": "fake_sns_arn",
            "stage5_threshold": "0.1",
            "survey": "BMI_SG",
            "threshold": "3",
            "top1_column": "largest_contributor",
            "top2_column": "second_largest_contributor",
            "total_columns": ["Q608_total", "Q606_other_gravel"],
            "total_steps": 6,
            "unique_identifier": ["responder_id"]
        }
}


##########################################################################################
#                                     Generic                                            #
##########################################################################################

@pytest.mark.parametrize(
    "which_lambda,which_runtime_variables,which_environment_variables,"
    "which_data,expected_message,assertion",
    [
        (lambda_wrangler_function, wrangler_runtime_variables,
         wrangler_environment_variables, None,
         "ClientError", test_generic_library.wrangler_assert)
    ])
@mock.patch('disclosure_wrangler.aws_functions.send_bpm_status')
def test_client_error(mock_send_bpm_status, which_lambda, which_runtime_variables,
                      which_environment_variables, which_data,
                      expected_message, assertion):
    test_generic_library.client_error(which_lambda, which_runtime_variables,
                                      which_environment_variables, which_data,
                                      expected_message, assertion)


@pytest.mark.parametrize(
    "which_lambda,which_runtime_variables,which_environment_variables,mockable_function,"
    "expected_message,assertion",
    [
        (lambda_method_function_1, method_runtime_variables_1,
         False, "stage1_method.RuntimeSchema",
         'Exception', test_generic_library.method_assert),
        (lambda_method_function_2, method_runtime_variables_1,
         False, "stage2_method.RuntimeSchema",
         'Exception', test_generic_library.method_assert),
        (lambda_method_function_5, method_runtime_variables_1,
         False, "stage5_method.RuntimeSchema",
         'Exception', test_generic_library.method_assert),
        (lambda_wrangler_function, wrangler_runtime_variables,
         wrangler_environment_variables, "disclosure_wrangler.EnvironmentSchema",
         'Exception', test_generic_library.wrangler_assert)
    ])
def test_general_error(which_lambda, which_runtime_variables,
                       which_environment_variables, mockable_function,
                       expected_message, assertion):
    test_generic_library.general_error(which_lambda, which_runtime_variables,
                                       which_environment_variables, mockable_function,
                                       expected_message, assertion)


@mock_s3
def test_incomplete_read_error():
    file_list = ["test_wrangler_input.json"]

    test_generic_library.incomplete_read_error(lambda_wrangler_function,
                                               wrangler_runtime_variables,
                                               wrangler_environment_variables,
                                               file_list,
                                               "disclosure_wrangler",
                                               "IncompleteReadError")


@pytest.mark.parametrize(
    "which_lambda,which_environment_variables,expected_message,assertion," +
    "which_runtime_variables",
    [
        (lambda_method_function_1, False,
         "KeyError", test_generic_library.method_assert, method_runtime_variables_1),
        (lambda_method_function_2, False,
         "KeyError", test_generic_library.method_assert, method_runtime_variables_2),
        (lambda_method_function_5, False,
         "KeyError", test_generic_library.method_assert, method_runtime_variables_5),
        (lambda_wrangler_function, wrangler_environment_variables,
         "KeyError", test_generic_library.wrangler_assert, None)
    ])
@mock.patch('disclosure_wrangler.aws_functions.send_bpm_status')
def test_key_error(mock_send_bpm_status, which_lambda, which_environment_variables,
                   expected_message, assertion, which_runtime_variables):
    if which_runtime_variables is None:
        test_generic_library.key_error(which_lambda,
                                       which_environment_variables,
                                       expected_message, assertion)
    else:
        with open("tests/fixtures/test_method_bad_input.json", "r") as file_1:
            file_data = file_1.read()
        prepared_data = pd.DataFrame(json.loads(file_data))
        print(prepared_data)
        which_runtime_variables[
            'RuntimeVariables']['data'] = prepared_data.to_json(orient="records")
        test_generic_library.key_error(which_lambda,
                                       which_environment_variables,
                                       expected_message, assertion,
                                       which_runtime_variables)


@mock_s3
def test_method_error():
    file_list = ["test_wrangler_input.json"]

    test_generic_library.wrangler_method_error(lambda_wrangler_function,
                                               wrangler_runtime_variables,
                                               wrangler_environment_variables,
                                               file_list,
                                               "disclosure_wrangler")


@pytest.mark.parametrize(
    "which_lambda,expected_message,assertion,which_environment_variables",
    [(lambda_method_function_1,
      "Error validating runtime param",
      test_generic_library.method_assert, {}),
     (lambda_method_function_2,
      "Error validating runtime param",
      test_generic_library.method_assert, {}),
     (lambda_method_function_5,
      "Error validating runtime param",
      test_generic_library.method_assert, {}),
     (lambda_wrangler_function,
      "Error validating environment param",
      test_generic_library.wrangler_assert, {}),
     (lambda_wrangler_function,
      "Error validating runtime param",
      test_generic_library.wrangler_assert, wrangler_environment_variables)])
def test_value_error(which_lambda, expected_message,
                     assertion, which_environment_variables):
    test_generic_library.value_error(which_lambda, expected_message, assertion,
                                     environment_variables=which_environment_variables)

##########################################################################################
#                                     Specific                                           #
##########################################################################################


@mock_s3
@pytest.mark.parametrize(
    "which_lambda,which_runtime,which_input_file,which_output_file",
    [(lambda_method_function_1,
      method_runtime_variables_1,
      "tests/fixtures/test_method_input.json",
      "tests/fixtures/test_method_1_prepared_output.json"),
     (lambda_method_function_2,
      method_runtime_variables_2,
      "tests/fixtures/test_method_1_prepared_output.json",
      "tests/fixtures/test_method_2_prepared_output.json"),
     (lambda_method_function_5,
      method_runtime_variables_5,
      "tests/fixtures/test_method_2_prepared_output.json",
      "tests/fixtures/test_method_5_prepared_output.json")
     ])
def test_disclosure(which_lambda, which_runtime, which_input_file, which_output_file):
    """
    Runs the method function.
    :param None
    :return Test Pass/Fail
    """
    with open(which_input_file, "r") as file_1:
        file_data = file_1.read()
    in_data = pd.DataFrame(json.loads(file_data))

    # Create the prefix/suffix for the column names that need passing in.
    beginning = which_runtime["RuntimeVariables"]["total_columns"][0] + "_"
    ending = "_" + which_runtime["RuntimeVariables"]["total_columns"][0]

    # Each step needs a different collection of pass in variables to run properly.
    if "top1_column" in which_runtime["RuntimeVariables"].keys():
        produced_data = which_lambda.disclosure(
            in_data,
            which_runtime["RuntimeVariables"]["disclosivity_marker"] + ending,
            which_runtime["RuntimeVariables"]["publishable_indicator"] + ending,
            which_runtime["RuntimeVariables"]["explanation"] + ending,
            which_runtime["RuntimeVariables"]["cell_total_column"] + ending,
            beginning + which_runtime["RuntimeVariables"]["top1_column"],
            beginning + which_runtime["RuntimeVariables"]["top2_column"],
            which_runtime["RuntimeVariables"]["threshold"])
    elif "parent_column" in which_runtime["RuntimeVariables"].keys():
        produced_data = which_lambda.disclosure(
            in_data,
            which_runtime["RuntimeVariables"]["disclosivity_marker"] + ending,
            which_runtime["RuntimeVariables"]["publishable_indicator"] + ending,
            which_runtime["RuntimeVariables"]["explanation"] + ending,
            which_runtime["RuntimeVariables"]["parent_column"],
            which_runtime["RuntimeVariables"]["threshold"])
    else:
        produced_data = which_lambda.disclosure(
            in_data,
            which_runtime["RuntimeVariables"]["disclosivity_marker"] + ending,
            which_runtime["RuntimeVariables"]["publishable_indicator"] + ending,
            which_runtime["RuntimeVariables"]["explanation"] + ending,
            which_runtime["RuntimeVariables"]["cell_total_column"] + ending)

    with open(which_output_file, "r") as file_2:
        file_data = file_2.read()
    prepared_data = pd.DataFrame(json.loads(file_data)).sort_index(axis=1)

    produced_data = produced_data.sort_index(axis=1)

    assert_frame_equal(produced_data, prepared_data)


@mock_s3
@pytest.mark.parametrize(
    "which_lambda,which_runtime,which_input_file,which_output_file",
    [(lambda_method_function_1,
      method_runtime_variables_1,
      "tests/fixtures/test_method_multi_input.json",
      "tests/fixtures/test_method_1_multi_prepared_output.json"),
     (lambda_method_function_2,
      method_runtime_variables_2,
      "tests/fixtures/test_method_1_multi_prepared_output.json",
      "tests/fixtures/test_method_2_multi_prepared_output.json"),
     (lambda_method_function_5,
      method_runtime_variables_5,
      "tests/fixtures/test_method_2_multi_prepared_output.json",
      "tests/fixtures/test_method_5_multi_prepared_output.json")
     ])
def test_method_success(which_lambda, which_runtime, which_input_file, which_output_file):
    """
    Runs the method function.
    :param None
    :return Test Pass/Fail
    """
    with open(which_input_file, "r") as file_1:
        file_data = file_1.read()
    in_data = pd.DataFrame(json.loads(file_data))

    which_runtime["RuntimeVariables"]["data"] = in_data.to_json(orient="records")

    output = which_lambda.lambda_handler(
        which_runtime, test_generic_library.context_object)

    produced_data = pd.DataFrame(json.loads(output["data"])).sort_index(axis=1)

    with open(which_output_file, "r") as file_2:
        file_data = file_2.read()
    prepared_data = pd.DataFrame(json.loads(file_data))

    assert output["success"]
    assert_frame_equal(produced_data, prepared_data)


@mock_s3
def test_wrangler_success_passed():
    """
    Runs the wrangler function.
    :param None
    :return Test Pass/Fail
    """
    bucket_name = wrangler_environment_variables["bucket_name"]
    client = test_generic_library.create_bucket(bucket_name)

    file_list = ["test_wrangler_input.json"]

    test_generic_library.upload_files(client, bucket_name, file_list)

    with mock.patch.dict(lambda_wrangler_function.os.environ,
                         wrangler_environment_variables):
        with mock.patch("disclosure_wrangler.boto3.client") as mock_client:
            mock_client_object = mock.Mock()
            mock_client.return_value = mock_client_object

            # Rather than mock the get/decode we tell the code that when the invoke is
            # called pass the variables to this replacement function instead.
            mock_client_object.invoke.side_effect = \
                test_generic_library.replacement_invoke

            # This stops the Error caused by the replacement function from stopping
            # the test.
            with pytest.raises(exception_classes.LambdaFailure):
                lambda_wrangler_function.lambda_handler(
                    wrangler_runtime_variables, test_generic_library.context_object
                )

        with open("tests/fixtures/test_method_input.json", "r") as file_1:
            test_data_prepared = file_1.read()
        prepared_data = pd.DataFrame(json.loads(test_data_prepared), dtype=float)

        with open("tests/fixtures/test_wrangler_to_method_input.json", "r") as file_2:
            test_data_produced = file_2.read()
        produced_data = pd.DataFrame(json.loads(test_data_produced), dtype=float)

        # Compares the data.
        assert_frame_equal(produced_data, prepared_data)

        with open("tests/fixtures/test_wrangler_to_method_runtime.json",
                  "r") as file_3:
            test_dict_prepared = file_3.read()
        produced_dict = json.loads(test_dict_prepared)

        # Ensures data is not in the RuntimeVariables and then compares.
        method_runtime_variables_1["RuntimeVariables"]["data"] = None
        assert produced_dict == method_runtime_variables_1["RuntimeVariables"]


@mock_s3
@mock.patch('disclosure_wrangler.aws_functions.save_to_s3',
            side_effect=test_generic_library.replacement_save_to_s3)
@mock.patch('disclosure_wrangler.aws_functions.save_dataframe_to_csv')
def test_wrangler_success_returned(mock_s3_put, mock_s3_csv):
    """
    Runs the wrangler function which calls the disclosure stages and returns the result.
    :param mock_s3_put - Replacement Function For The Data Saving AWS Functionality.
    :param mock_s3_csv - Mock Out Secondary Save As Unneeded.
    :return Test Pass/Fail
    """
    bucket_name = wrangler_environment_variables["bucket_name"]
    client = test_generic_library.create_bucket(bucket_name)

    file_list = ["test_wrangler_input.json"]

    test_generic_library.upload_files(client, bucket_name, file_list)

    with open("tests/fixtures/test_method_5_prepared_output.json", "r") as file_1:
        test_data_5_out = file_1.read()

    with mock.patch.dict(lambda_wrangler_function.os.environ,
                         wrangler_environment_variables):
        with mock.patch("disclosure_wrangler.boto3.client") as mock_client:
            mock_client_object = mock.Mock()
            mock_client.return_value = mock_client_object

            mock_client_object.invoke.return_value.get.return_value.read \
                .return_value.decode.return_value = json.dumps({
                 "data": test_data_5_out,
                 "success": True,
                 "anomalies": []
                })

            output = lambda_wrangler_function.lambda_handler(
                wrangler_runtime_variables, test_generic_library.context_object
            )

    with open("tests/fixtures/test_wrangler_prepared_output.json", "r") as file_2:
        test_data_prepared = file_2.read()
    prepared_data = pd.DataFrame(json.loads(test_data_prepared))

    with open("tests/fixtures/" +
              wrangler_runtime_variables["RuntimeVariables"]["out_file_name"],
              "r") as file_3:
        test_data_produced = file_3.read()
    produced_data = pd.DataFrame(json.loads(test_data_produced))

    assert output
    assert_frame_equal(produced_data, prepared_data)
