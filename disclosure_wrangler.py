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
    in_file_name = fields.Str(required=True)
    incoming_message_group = fields.Str(required=True)
    method_name = fields.Str(required=True)
    out_file_name = fields.Str(required=True)
    sns_topic_arn = fields.Str(required=True)
    sqs_message_group_id = fields.Str(required=True)
    csv_file_name = fields.Str(required=True)


def lambda_handler(event, context):
    """
    Responsible for executing specified disclosure methods, masking values which could
    be used to identify specific responders.
    :param event: JSON payload containing:
    RuntimeVariables:{
        disclosivity_marker: The name of the column to put 'disclosive' marker.
        publishable_indicator: The name of the column to put 'publish' marker.
        explanation: The name of the column to put reason for pass/fail.
        total_column: The name of the column holding the cell total.
        parent_column: The name of the column holding the count of parent company.
        threshold: The threshold used in the disclosure steps.
        cell_total_column: The name of the column holding the cell total.
        top1_column: The name of the column largest contributor to the cell.
        top2_column: The name of the column second largest contributor to the cell.
        stage5_threshold: The threshold used in the disclosure calculation.
        disclosure_stages: The stages of disclosure you wish to run e.g. (1 2 5)
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

        checkpoint = config['checkpoint']
        bucket_name = config['bucket_name']
        in_file_name = config['in_file_name']
        incoming_message_group = config['incoming_message_group']
        method_name = config["method_name"]
        out_file_name = config['out_file_name']
        sns_topic_arn = config["sns_topic_arn"]
        sqs_message_group_id = config["sqs_message_group_id"]
        csv_file_name = config["csv_file_name"]

        # Runtime Variables
        disclosivity_marker = event['RuntimeVariables']["disclosivity_marker"]
        publishable_indicator = event['RuntimeVariables']["publishable_indicator"]
        explanation = event['RuntimeVariables']["explanation"]
        total_column = event['RuntimeVariables']["total_column"]
        parent_column = event['RuntimeVariables']["parent_column"]
        threshold = event['RuntimeVariables']["threshold"]
        cell_total_column = event['RuntimeVariables']["cell_total_column"]
        top1_column = event['RuntimeVariables']["top1_column"]
        top2_column = event['RuntimeVariables']["top2_column"]
        stage5_threshold = event['RuntimeVariables']["stage5_threshold"]
        disclosure_stages = event['RuntimeVariables']["disclosure_stages"]
        sqs_queue_url = event['RuntimeVariables']["queue_url"]

        # Set up clients
        sqs = boto3.client("sqs", "eu-west-2")
        lambda_client = boto3.client("lambda", "eu-west-2")

        data, receipt_handle = aws_functions.get_dataframe(sqs_queue_url, bucket_name,
                                                           in_file_name,
                                                           incoming_message_group)
        logger.info("Successfully retrieved data")

        data[disclosivity_marker] = None
        data[publishable_indicator] = None
        data[explanation] = None

        formatted_data = data.to_json(orient="records")

        disclosure_stages_list = disclosure_stages.split()
        disclosure_stages_list.sort()

        generic_json_payload = {
            "json_data": formatted_data,
            "disclosivity_marker": disclosivity_marker,
            "publishable_indicator": publishable_indicator,
            "explanation": explanation,
        }

        stage1_payload = {
            "total_column": total_column
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
            }

        aws_functions.save_data(bucket_name, out_file_name, formatted_data['data'],
                                sqs_queue_url, sqs_message_group_id)

        logger.info("Successfully sent data to s3")

        output_data = formatted_data['data']

        aws_functions.write_dataframe_to_csv(pd.read_json(output_data, dtype=False),
                                             bucket_name, csv_file_name)

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
