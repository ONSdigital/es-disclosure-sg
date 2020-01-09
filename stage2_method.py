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
    parent_column = marshmallow.fields.Str(required=True)
    threshold = marshmallow.fields.Str(required=True)
    json_data = marshmallow.fields.Str(required=True)


def lambda_handler(event, context):
    """
    Main entry point into method
    :param event: json payload containing:
            json_data: input data.
            disclosivity_marker: The name of the column to put 'disclosive' marker.
            publishable_indicator: The name of the column to put 'publish' marker.
            explanation: The name of the column to put reason for pass/fail.
            parent_column: The name of the column holding the count of parent company.
            threshold: The threshold above which a row is not disclosive.
    :param context: AWS Context Object.
    :return final_output: Dict containing either:
            {"success": True, "data": <stage 2 output - json >}
            {"success": False, "error": <error message - string>}
    """
    current_module = "Disclosure Stage 2 Method"
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
        parent_column = config['parent_column']
        threshold = int(config['threshold'])

        input_json = json.loads(config['json_data'])

        input_dataframe = pd.DataFrame(input_json)

        disclosure_output = disclosure(input_dataframe,
                                       disclosivity_marker,
                                       publishable_indicator,
                                       explanation,
                                       parent_column,
                                       threshold)
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
               explanation, parent_column, threshold):
    """
    Takes in a dataframe and applies the stage2 disclosure rule.
    :param input_df: input data.
    :param disclosivity_marker: The name of the column to put 'disclosive' marker.
    :param publishable_indicator: The name of the column to put 'publish' marker.
    :param explanation: The name of the column to put reason for pass/fail.
    :param parent_column: The name of the column holding the count of parent company.
    :param threshold: The threshold above which a row is not disclosive.
    :return output_df: Input dataframe with the addition of stage2 disclosure info.
    """
    def run_disclosure(row):
        if row[publishable_indicator] != 'Publish':
            if row[parent_column] < threshold:
                row[disclosivity_marker] = 'Yes'
                row[publishable_indicator] = 'No'
                row[explanation] = 'Stage 2 - Only '\
                                   + str(row[parent_column])\
                                   + " parent references in cell"
            else:
                row[disclosivity_marker] = 'No'
                row[publishable_indicator] = 'Not Applicable'
                row[explanation] = 'Passed Stage 2'
        return row

    output_df = input_df.apply(run_disclosure, axis=1)

    return output_df
