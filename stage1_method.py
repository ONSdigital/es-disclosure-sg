import json
import logging

import marshmallow
import pandas as pd


class EnvironSchema(marshmallow.Schema):
    """
    Class to set up the environment variables schema.
    """
    disclosivity_marker = marshmallow.fields.Str(required=True)
    publishable_indicator = marshmallow.fields.Str(required=True)
    explanation = marshmallow.fields.Str(required=True)
    total_column = marshmallow.fields.Str(required=True)
    json_data = marshmallow.fields.Str(required=True)


def lambda_handler(event, context):
    """
    Main entry point into method
    :param event: json payload containing:
            json_data: input data.
            disclosivity_marker: The name of the column to put 'disclosive' marker.
            publishable_indicator: The name of the column to put 'publish' marker.
            explanation: The name of the column to put reason for pass/fail.
            total_column: The name of the column holding the cell total.
    :param context: AWS Context Object.
    :return final_output: Dict containing either:
            {"success": True, "data": <stage 1 output - json >}
            {"success": False, "error": <error message - string>}
    """
    current_module = "Disclosure Stage 1 Method"
    error_message = ''
    log_message = ''
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    try:
        # Set up Environment variables Schema.
        schema = EnvironSchema(strict=False)
        config, errors = schema.load(event)
        if errors:
            raise ValueError(f"Error validating environment parameters: {errors}")

        logger.info("Validated params")

        disclosivity_marker = config['disclosivity_marker']
        publishable_indicator = config['publishable_indicator']
        explanation = config['explanation']
        total_column = config['total_column']

        input_json = json.loads(config['json_data'])

        input_dataframe = pd.DataFrame(input_json)

        disclosure_output = disclosure(input_dataframe,
                                       disclosivity_marker,
                                       publishable_indicator,
                                       explanation,
                                       total_column)
        logger.info("Successfully completed Disclosure")
        final_output = {"data": disclosure_output.to_json(orient='records')}

    except ValueError as e:
        error_message = (
            "Parameter validation error in "
            + current_module
            + " |- "
            + str(e.args)
            + " | Request ID: "
            + str(context.aws_request_id)
        )
        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)

    except Exception as e:
        error_message = (
            "General Error in "
            + current_module
            + " ("
            + str(type(e))
            + ") |- "
            + str(e.args)
            + " | Request ID: "
            + str(context.aws_request_id)
        )
        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    finally:
        if (len(error_message)) > 0:
            logger.error(log_message)
            return {"success": False, "error": error_message}
    logger.info("Successfully completed module: " + current_module)
    final_output['success'] = True
    return final_output


def disclosure(input_df, disclosivity_marker, publishable_indicator,
               explanation, total_column):
    """
    Takes in a dataframe and applies the stage1 disclosure rule.
    :param input_df: input data.
    :param disclosivity_marker: The name of the column to put 'disclosive' marker.
    :param publishable_indicator: The name of the column to put 'publish' marker.
    :param explanation: The name of the column to put reason for pass/fail.
    :param total_column: The name of the column holding the cell total.
    :return output_df: Input dataframe with the addition of stage1 disclosure info.
    """

    def run_disclosure(row):

        if row[total_column] == 0:
            row[disclosivity_marker] = 'No'
            row[publishable_indicator] = 'Publish'
            row[explanation] = 'Stage 1 - Total column is 0'
        else:
            row[disclosivity_marker] = 'Yes'
            row[publishable_indicator] = 'Not Applicable'
            row[explanation] = 'Through stage 1'
        return row

    output_df = input_df.apply(run_disclosure, axis=1)

    return output_df
