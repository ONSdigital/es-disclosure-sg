import json
import logging
import os

import boto3
import pandas as pd
from es_aws_functions import aws_functions, exception_classes, general_functions
from marshmallow import EXCLUDE, Schema, fields


class EnvironmentSchema(Schema):
    class Meta:
        unknown = EXCLUDE

    def handle_error(self, e, data, **kwargs):
        logging.error(f"Error validating environment params: {e}")
        raise ValueError(f"Error validating environment params: {e}")

    checkpoint = fields.Str(required=True)
    bucket_name = fields.Str(required=True)
    method_name = fields.Str(required=True)


class RuntimeSchema(Schema):
    class Meta:
        unknown = EXCLUDE

    def handle_error(self, e, data, **kwargs):
        logging.error(f"Error validating runtime params: {e}")
        raise ValueError(f"Error validating runtime params: {e}")

    cell_total_column = fields.Str(required=True)
    disclosivity_marker = fields.Str(required=True)
    disclosure_stages = fields.Str(required=True)
    explanation = fields.Str(required=True)
    final_output_location = fields.Str(required=True)
    in_file_name = fields.Str(required=True)
    out_file_name = fields.Str(required=True)
    parent_column = fields.Str(required=True)
    publishable_indicator = fields.Str(required=True)
    sns_topic_arn = fields.Str(required=True)
    stage5_threshold = fields.Str(required=True)
    threshold = fields.Str(required=True)
    top1_column = fields.Str(required=True)
    top2_column = fields.Str(required=True)
    total_columns = fields.List(fields.String, required=True)
    unique_identifier = fields.List(fields.String, required=True)


def lambda_handler(event, context):
    """
    Responsible for executing specified disclosure methods, masking values which could
    be used to identify specific responders.
    :param event: JSON payload containing:
    RuntimeVariables:{
        cell_total_column: The name of the column holding the cell total.
        disclosivity_marker: The name of the column to put "disclosive" marker.
        disclosure_stages: The stages of disclosure you wish to run e.g. (1 2 5)
        explanation: The name of the column to put reason for pass/fail.
        in_file_name: Input file specified.
        out_file_name: Output file specified.
        parent_column: The name of the column holding the count of parent company.
        publishable_indicator: The name of the column to put "publish" marker.
        stage5_threshold: The threshold used in the disclosure calculation.
        threshold: The threshold used in the disclosure steps.
        top1_column: The name of the column largest contributor to the cell.
        top2_column: The name of the column second largest contributor to the cell.
        total_column: The name of the column holding the cell total.
        unique_identifier: A list of the column names to specify a unique cell.
    }
    :param context: AWS Context Object.
    :return final_output: Dict containing either:
        {"success": True, "checkpoint": <output - Type: String>}
        {"success": False, "error": <error message - Type: String>}
    """
    logger = logging.getLogger("Disclosure Logger")
    logger.setLevel(logging.INFO)
    current_module = "Disclosure Wrangler"
    error_message = ""
    # Define run_id outside of try block
    run_id = 0
    try:
        # Retrieve run_id before input validation
        # Because it is used in exception handling
        run_id = event["RuntimeVariables"]["run_id"]

        environment_variables = EnvironmentSchema().load(os.environ)

        runtime_variables = RuntimeSchema().load(event["RuntimeVariables"])

        logger.info("Validated parameters.")

        # Environment Variables
        checkpoint = environment_variables["checkpoint"]
        bucket_name = environment_variables["bucket_name"]
        method_name = environment_variables["method_name"]

        # Runtime Variables
        cell_total_column = runtime_variables["cell_total_column"]
        disclosivity_marker = runtime_variables["disclosivity_marker"]
        disclosure_stages = runtime_variables["disclosure_stages"]
        explanation = runtime_variables["explanation"]
        final_output_location = runtime_variables["final_output_location"]
        in_file_name = runtime_variables["in_file_name"]
        out_file_name = runtime_variables["out_file_name"]
        parent_column = runtime_variables["parent_column"]
        publishable_indicator = runtime_variables["publishable_indicator"]
        sns_topic_arn = runtime_variables["sns_topic_arn"]
        stage5_threshold = runtime_variables["stage5_threshold"]
        threshold = runtime_variables["threshold"]
        top1_column = runtime_variables["top1_column"]
        top2_column = runtime_variables["top2_column"]
        total_columns = runtime_variables["total_columns"]
        unique_identifier = runtime_variables["unique_identifier"]

        # Set up clients
        lambda_client = boto3.client("lambda", "eu-west-2")

        data = aws_functions.read_dataframe_from_s3(bucket_name, in_file_name)
        logger.info("Successfully retrieved data")

        formatted_data = data.to_json(orient="records")

        disclosure_stages_list = disclosure_stages.split()
        disclosure_stages_list.sort()

        generic_json_payload = {
            "data": formatted_data,
            "disclosivity_marker": disclosivity_marker,
            "publishable_indicator": publishable_indicator,
            "explanation": explanation,
            "total_columns": total_columns,
            "unique_identifier": unique_identifier,
            "run_id": run_id
        }

        stage1_payload = {
            "cell_total_column": cell_total_column
        }

        stage2_payload = {
            "parent_column": parent_column,
            "threshold": threshold
        }

        stage3_payload = {
        }

        stage4_payload = {
        }

        stage5_payload = {
            "top1_column": top1_column,
            "top2_column": top2_column,
            "cell_total_column": cell_total_column,
            "threshold": stage5_threshold
        }

        for disclosure_step in disclosure_stages_list:

            payload_array = [generic_json_payload, stage1_payload, stage2_payload,
                             stage3_payload, stage4_payload, stage5_payload]

            # Find the specific location where the stage number need to be inserted and
            # constructs the relevant method name using the disclosure stage number.
            index = method_name.find("-method")
            lambda_name = method_name[:index] + disclosure_step + method_name[index:]

            # Combines the generic payload and the stage specific payload.
            combined_input = {**payload_array[0], **(payload_array[int(disclosure_step)])}
            combined_input = {"RuntimeVariables": combined_input}

            formatted_data = invoke_method(lambda_name,
                                           combined_input,
                                           lambda_client)

            if not formatted_data["success"]:
                raise exception_classes.MethodFailure(formatted_data["error"])

            logger.info("Successfully invoked stage " + disclosure_step + " lambda")

            # Located here as after the first loop it requires formatted data to be
            # referenced with "data" and the JSON needs to be reset to use the right data.
            generic_json_payload = {
                "data": formatted_data["data"],
                "disclosivity_marker": disclosivity_marker,
                "publishable_indicator": publishable_indicator,
                "explanation": explanation,
                "total_columns": total_columns,
                "unique_identifier": unique_identifier,
                "run_id": run_id
            }

        aws_functions.save_to_s3(bucket_name, out_file_name, formatted_data["data"])

        logger.info("Successfully sent data to s3")

        output_data = formatted_data["data"]

        aws_functions.save_dataframe_to_csv(pd.read_json(output_data, dtype=False),
                                            bucket_name, final_output_location)

        aws_functions.send_sns_message(checkpoint, sns_topic_arn, "Disclosure")
        logger.info("Successfully sent message to sns")

    except Exception as e:
        error_message = general_functions.handle_exception(e, current_module,
                                                           run_id, context)
    finally:
        if (len(error_message)) > 0:
            logger.error(error_message)
            raise exception_classes.LambdaFailure(error_message)

    logger.info("Successfully completed module: " + current_module)

    return {"success": True, "checkpoint": checkpoint}


def invoke_method(lambda_execution_name, payload, lambda_client):
    """
    Invokes the given lambda, using the provided name and payload and translates it
    into JSON.
    :param lambda_execution_name: Name of the lambda you wish to execute - Type: String
    :param payload: The Payload you are sending into the method - Type: JSON
    :param lambda_client: The client object - Type: Service client instance
    :return: formatted_data: The returned results from the method execution - Type: JSON
    """
    returned_data = lambda_client.invoke(
        FunctionName=lambda_execution_name, Payload=json.dumps(payload)
    )

    formatted_data = json.loads(returned_data.get("Payload").read().decode("UTF-8"))

    return formatted_data
