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
    top1_column = marshmallow.fields.Str(required=True)
    top2_column = marshmallow.fields.Str(required=True)
    cell_total_column = marshmallow.fields.Str(required=True)
    json_data = marshmallow.fields.Str(required=True)
    threshold = marshmallow.fields.Str(required=True)
    reference = marshmallow.fields.Str(required=True)
    total_columns = marshmallow.fields.List(marshmallow.fields.Str(), required=True)


def lambda_handler(event, context):
    """
    Main entry point into method
    :param event: json payload containing:
            json_data: input data.
            disclosivity_marker: The name of the column to put 'disclosive' marker.
            publishable_indicator: The name of the column to put 'publish' marker.
            explanation: The name of the column to put reason for pass/fail.
            total_column: The name of the column holding the cell total.
            top1_column: The name of the column largest contributor to the cell.
            top2_column: The name of the column second largest contributor to the cell.
            threshold: The threshold used in the disclosure calculation.
            total_columns: The names of the columns holding the cell totals.
                        Included so that correct disclosure columns used.
    :param context: AWS Context Object.
    :return final_output: Dict containing either:
            {"success": True, "data": <stage 5 output - json >}
            {"success": False, "error": <error message - string>}
    """
    current_module = "Disclosure Stage 5 Method"
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
        top1_column = config['top1_column']
        top2_column = config['top2_column']
        cell_total_column = config['cell_total_column']
        threshold = config['threshold']
        total_columns = config['total_columns']
        reference = config['reference']

        input_json = json.loads(config['json_data'])

        input_dataframe = pd.DataFrame(input_json)
        stage_5_output = pd.DataFrame()
        counter = 0
        for total_column in total_columns:
            this_disclosivity_marker = disclosivity_marker + "_" + total_column
            this_publishable_indicator = publishable_indicator + "_" + total_column
            this_explanation = explanation + "_" + total_column
            this_top1_column = total_column + "_" + top1_column
            this_top2_column = total_column + "_" + top2_column
            this_cell_total_column = cell_total_column + "_" + total_column
            disclosure_output = disclosure(input_dataframe,
                                           this_disclosivity_marker,
                                           this_publishable_indicator,
                                           this_explanation,
                                           this_cell_total_column,
                                           this_top1_column, this_top2_column,
                                           threshold)
            if (counter == 0):
                stage_5_output = disclosure_output
            else:
                stage_5_output = stage_5_output.merge(disclosure_output,
                                                      on=reference, how="left")
            counter += 1
            logger.info("Successfully completed Disclosure stage 5 for:"
                        + str(total_column))

        logger.info("Successfully completed Disclosure")

        final_output = {"data": stage_5_output.to_json(orient='records')}

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
               explanation, cell_total_column, top1_column, top2_column, threshold):
    """
    Takes in a dataframe and applies the stage5 disclosure rule.
    :param input_df: input data.
    :param disclosivity_marker: The name of the column to put 'disclosive' marker.
    :param publishable_indicator: The name of the column to put 'publish' marker.
    :param explanation: The name of the column to put reason for pass/fail.
    :param cell_total_column: The name of the column holding the cell total.
    :param top1_column: The name of the column largest contributor to the cell.
    :param top2_column: The name of the column second largest contributor to the cell.
    :param threshold: The threshold used in the disclosure calculation.
    :return output_df: Input dataframe with the addition of stage5 disclosure info.
    """
    def run_disclosure(row):
        if row[publishable_indicator] not in ('Publish', 'No'):
            row['Score'] = \
                (row[cell_total_column] - row[top1_column] - row[top2_column]) \
                / row[top1_column]
            if row['Score'] >= float(threshold):
                row[disclosivity_marker] = 'No'
                row[publishable_indicator] = 'Publish'
                row[explanation] = "Stage 5 - Score is " + str(row['Score'])\
                                   + ". This meets threshold of (>=" + threshold\
                                   + ")"
            else:
                row[disclosivity_marker] = 'Yes'
                row[publishable_indicator] = 'No'
                row[explanation] = "Stage 5 - Score is " + str(row['Score'])\
                                   + ". This doesnt meet threshold of (>=" + threshold\
                                   + ")"
        return row

    output_df = input_df.apply(run_disclosure, axis=1)

    return output_df
