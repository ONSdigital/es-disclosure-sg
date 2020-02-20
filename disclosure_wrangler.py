import json
import logging
import os

import boto3
import pandas as pd
from botocore.exceptions import ClientError, IncompleteReadError
from es_aws_functions import aws_functions, exception_classes
from marshmallow import Schema, fields


class EnvironSchema(Schema):
    """
    Class to set up the environment variables schema.
    """
    checkpoint = fields.Str(required=True)
    bucket_name = fields.Str(required=True)
    method_name = fields.Str(required=True)
    sns_topic_arn = fields.Str(required=True)


def lambda_handler(event, context):
    """
    Responsible for executing specified disclosure methods, masking values which could
    be used to identify specific responders.
    :param event: JSON payload containing:
    RuntimeVariables:{
        cell_total_column: The name of the column holding the cell total.
        disclosivity_marker: The name of the column to put 'disclosive' marker.
        disclosure_stages: The stages of disclosure you wish to run e.g. (1 2 5)
        explanation: The name of the column to put reason for pass/fail.
        in_file_name: Input file specified.
        incoming_message_group_id: Input ID specified.
        out_file_name: Output file specified.
        outgoing_message_group_id: Output ID specified.
        parent_column: The name of the column holding the count of parent company.
        publishable_indicator: The name of the column to put 'publish' marker.
        sqs_queue_url: The URL of the sqs queue used for the run.
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
    log_message = ""
    # Define run_id outside of try block
    run_id = 0
    try:
        # Retrieve run_id before input validation
        # Because it is used in exception handling
        run_id = event['RuntimeVariables']['run_id']

        schema = EnvironSchema(strict=False)
        config, errors = schema.load(os.environ)
        if errors:
            raise ValueError(f"Error validating environment parameters: {errors}")
        logger.info("Validated params")

        # Environment Variables
        checkpoint = config['checkpoint']
        bucket_name = config['bucket_name']
        method_name = config["method_name"]
        sns_topic_arn = config["sns_topic_arn"]

        # Runtime Variables
        cell_total_column = event['RuntimeVariables']["cell_total_column"]
        disclosivity_marker = event['RuntimeVariables']["disclosivity_marker"]
        disclosure_stages = event['RuntimeVariables']["disclosure_stages"]
        explanation = event['RuntimeVariables']["explanation"]
        in_file_name = event['RuntimeVariables']['in_file_name']
        incoming_message_group_id = event['RuntimeVariables']['incoming_message_group_id']
        out_file_name = event['RuntimeVariables']['out_file_name']
        outgoing_message_group_id = event['RuntimeVariables']["outgoing_message_group_id"]
        parent_column = event['RuntimeVariables']["parent_column"]
        publishable_indicator = event['RuntimeVariables']["publishable_indicator"]
        sqs_queue_url = event['RuntimeVariables']["queue_url"]
        stage5_threshold = event['RuntimeVariables']["stage5_threshold"]
        threshold = event['RuntimeVariables']["threshold"]
        top1_column = event['RuntimeVariables']["top1_column"]
        top2_column = event['RuntimeVariables']["top2_column"]
        total_columns = event['RuntimeVariables']["total_columns"]
        unique_identifier = event['RuntimeVariables']["unique_identifier"]

        # Set up clients
        sqs = boto3.client("sqs", "eu-west-2")
        lambda_client = boto3.client("lambda", "eu-west-2")

        data, receipt_handle = aws_functions.get_dataframe(sqs_queue_url, bucket_name,
                                                           in_file_name,
                                                           incoming_message_group_id,
                                                           run_id)
        logger.info("Successfully retrieved data")

        formatted_data = data.to_json(orient="records")

        disclosure_stages_list = disclosure_stages.split()
        disclosure_stages_list.sort()

        generic_json_payload = {
            "json_data": formatted_data,
            "disclosivity_marker": disclosivity_marker,
            "publishable_indicator": publishable_indicator,
            "explanation": explanation,
            "total_columns": total_columns,
            "unique_identifier": unique_identifier
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
            index = method_name.find('-method')
            lambda_name = method_name[:index] + disclosure_step + method_name[index:]

            # Combines the generic payload and the stage specific payload.
            combined_input = {**payload_array[0], **(payload_array[int(disclosure_step)])}

            formatted_data = invoke_method(lambda_name,
                                           combined_input,
                                           lambda_client)

            if not formatted_data['success']:
                raise exception_classes.MethodFailure(formatted_data['error'])

            logger.info("Successfully invoked stage " + disclosure_step + " lambda")

            # Located here as after the first loop it requires formatted data to be
            # referenced with 'data' and the JSON needs to be reset to use the right data.
            generic_json_payload = {
                "json_data": formatted_data['data'],
                "disclosivity_marker": disclosivity_marker,
                "publishable_indicator": publishable_indicator,
                "explanation": explanation,
                "total_columns": total_columns,
                "unique_identifier": unique_identifier
            }

        aws_functions.save_data(bucket_name, out_file_name, formatted_data['data'],
                                sqs_queue_url, outgoing_message_group_id, run_id)

        logger.info("Successfully sent data to s3")

        output_data = formatted_data['data']

        aws_functions.save_dataframe_to_csv(pd.read_json(output_data, dtype=False),
                                            bucket_name, out_file_name)

        if receipt_handle:
            sqs.delete_message(QueueUrl=sqs_queue_url, ReceiptHandle=str(receipt_handle))
            logger.info("Successfully deleted input data from s3")

        aws_functions.send_sns_message(checkpoint, sns_topic_arn, "Disclosure")
        logger.info("Successfully sent message to sns")

    except AttributeError as e:
        error_message = ("Bad data encountered in "
                         + current_module + " |- "
                         + str(e.args) + " | Request ID: "
                         + str(context.aws_request_id)
                         + " | Run_id: " + str(run_id))

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    except ValueError as e:
        error_message = ("Parameter validation error in "
                         + current_module + " |- "
                         + str(e.args) + " | Request ID: "
                         + str(context.aws_request_id)
                         + " | Run_id: " + str(run_id))

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    except ClientError as e:
        error_message = ("AWS Error ("
                         + str(e.response["Error"]["Code"]) + ") "
                         + current_module + " |- "
                         + str(e.args) + " | Request ID: "
                         + str(context.aws_request_id)
                         + " | Run_id: " + str(run_id))

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    except KeyError as e:
        error_message = ("Key Error in "
                         + current_module + " |- "
                         + str(e.args) + " | Request ID: "
                         + str(context.aws_request_id)
                         + " | Run_id: " + str(run_id))

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    except IncompleteReadError as e:
        error_message = ("Incomplete Lambda response encountered in "
                         + current_module + " |- "
                         + str(e.args) + " | Request ID: "
                         + str(context.aws_request_id)
                         + " | Run_id: " + str(run_id))

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    except exception_classes.MethodFailure as e:
        error_message = e.error_message

        log_message = "Error in " + method_name + "." \
                      + " | Run_id: " + str(run_id)
    except Exception as e:
        error_message = ("General Error in "
                         + current_module + " ("
                         + str(type(e)) + ") |- "
                         + str(e.args) + " | Request ID: "
                         + str(context.aws_request_id)
                         + " | Run_id: " + str(run_id))

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    finally:
        if (len(error_message)) > 0:
            logger.error(log_message)
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
