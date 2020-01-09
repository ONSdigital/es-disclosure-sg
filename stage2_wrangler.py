import json
import logging
import os

import boto3
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
    sqs_queue_url = fields.Str(required=True)


def lambda_handler(event, context):
    """
    Main entry point into wrangler
    :param event: json payload containing:
        RuntimeVariables:{
            disclosivity_marker: The name of the column to put 'disclosive' marker.
            publishable_indicator: The name of the column to put 'publish' marker.
            explanation: The name of the column to put reason for pass/fail.
            parent_column: The name of the column holding the count of parent company.
            threshold: The threshold above which a row is not disclosive.
        }
    :param context: AWS Context Object.
    :return: Success - True/False & Checkpoint/error
    """
    logger = logging.getLogger("Disclosure Logger")
    logger.setLevel(logging.INFO)
    current_module = "Disclosure Stage 2 Wrangler"

    error_message = ""
    log_message = ""
    try:
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
        sqs_queue_url = config["sqs_queue_url"]

        # Runtime Variables
        disclosivity_marker = event['RuntimeVariables']["disclosivity_marker"]
        publishable_indicator = event['RuntimeVariables']["publishable_indicator"]
        explanation = event['RuntimeVariables']["explanation"]
        parent_column = event['RuntimeVariables']["parent_column"]
        threshold = event['RuntimeVariables']["threshold"]

        # Set up clients
        sqs = boto3.client("sqs", "eu-west-2")
        lambda_client = boto3.client("lambda", "eu-west-2")

        data, receipt_handle = aws_functions.get_dataframe(sqs_queue_url, bucket_name,
                                                           in_file_name,
                                                           incoming_message_group)
        logger.info("Successfully retrieved data")

        formatted_data = data.to_json(orient="records")
        json_payload = {
            "json_data": formatted_data,
            "disclosivity_marker": disclosivity_marker,
            "publishable_indicator": publishable_indicator,
            "explanation": explanation,
            "parent_column": parent_column,
            "threshold": threshold
        }

        returned_data = lambda_client.invoke(
            FunctionName=method_name, Payload=json.dumps(json_payload)
        )
        json_response = json.loads(returned_data.get("Payload").read().decode("UTF-8"))
        if not json_response['success']:
            raise exception_classes.MethodFailure(json_response['error'])
        logger.info("Successfully invoked lambda")

        aws_functions.save_data(bucket_name, out_file_name,
                                json_response['data'], sqs_queue_url,
                                sqs_message_group_id)
        logger.info("Successfully sent data to s3")

        if receipt_handle:
            sqs.delete_message(QueueUrl=sqs_queue_url, ReceiptHandle=receipt_handle)
        logger.info("Successfully deleted input data from s3")
        logger.info(aws_functions.delete_data(bucket_name, in_file_name))

        aws_functions.send_sns_message(checkpoint, sns_topic_arn, "Stage 2 Disclosure.")
        logger.info("Successfully sent message to sns")

    except AttributeError as e:
        error_message = ("Bad data encountered in "
                         + current_module + " |- "
                         + str(e.args) + " | Request ID: "
                         + str(context.aws_request_id))

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    except ValueError as e:
        error_message = ("Parameter validation error in "
                         + current_module + " |- "
                         + str(e.args) + " | Request ID: "
                         + str(context.aws_request_id))

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    except ClientError as e:
        error_message = ("AWS Error ("
                         + str(e.response["Error"]["Code"]) + ") "
                         + current_module + " |- "
                         + str(e.args) + " | Request ID: "
                         + str(context.aws_request_id))

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    except KeyError as e:
        error_message = ("Key Error in "
                         + current_module + " |- "
                         + str(e.args) + " | Request ID: "
                         + str(context.aws_request_id))

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    except IncompleteReadError as e:
        error_message = ("Incomplete Lambda response encountered in "
                         + current_module + " |- "
                         + str(e.args) + " | Request ID: "
                         + str(context.aws_request_id))

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    except exception_classes.MethodFailure as e:
        error_message = e.error_message
        log_message = "Error in " + method_name + "."
    except Exception as e:
        error_message = ("General Error in "
                         + current_module + " ("
                         + str(type(e)) + ") |- "
                         + str(e.args) + " | Request ID: "
                         + str(context.aws_request_id))

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    finally:
        if (len(error_message)) > 0:
            logger.error(log_message)
            return {"success": False, "error": error_message}

    logger.info("Successfully completed module: " + current_module)
    return {"success": True, "checkpoint": checkpoint}
