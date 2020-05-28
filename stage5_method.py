import json
import logging

import pandas as pd
from es_aws_functions import general_functions
from marshmallow import EXCLUDE, Schema, fields


class RuntimeSchema(Schema):
    class Meta:
        unknown = EXCLUDE

    def handle_error(self, e, data, **kwargs):
        logging.error(f"Error validating runtime params: {e}")
        raise ValueError(f"Error validating runtime params: {e}")

    disclosivity_marker = fields.Str(required=True)
    publishable_indicator = fields.Str(required=True)
    explanation = fields.Str(required=True)
    top1_column = fields.Str(required=True)
    top2_column = fields.Str(required=True)
    cell_total_column = fields.Str(required=True)
    data = fields.Str(required=True)
    threshold = fields.Str(required=True)
    unique_identifier = fields.List(fields.Str(), required=True)
    total_columns = fields.List(fields.Str(), required=True)
    run_id = fields.Str(required=True)


def lambda_handler(event, context):
    """
    Main entry point into method
    :param event: json payload containing:
            data: input data.
            disclosivity_marker: The name of the column to put "disclosive" marker.
            publishable_indicator: The name of the column to put "publish" marker.
            explanation: The name of the column to put reason for pass/fail.
            total_column: The name of the column holding the cell total.
            top1_column: The name of the column largest contributor to the cell.
            top2_column: The name of the column second largest contributor to the cell.
            threshold: The threshold used in the disclosure calculation.
            total_columns: The names of the columns holding the cell totals.
                        Included so that correct disclosure columns used.
            unique_identifier: The name of the column holding the contributor id.
    :param context: AWS Context Object.
    :return final_output: Dict containing either:
            {"success": True, "data": <stage 5 output - json >}
            {"success": False, "error": <error message - string>}
    """
    current_module = "Disclosure Stage 5 Method"
    error_message = ""
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    # Define run_id outside of try block
    run_id = 0
    try:
        # Retrieve run_id before input validation
        # Because it is used in exception handling
        run_id = event["RuntimeVariables"]["run_id"]

        runtime_variables = RuntimeSchema().load(event["RuntimeVariables"])

        logger.info("Validated parameters.")

        # Runtime Variables
        disclosivity_marker = runtime_variables["disclosivity_marker"]
        publishable_indicator = runtime_variables["publishable_indicator"]
        explanation = runtime_variables["explanation"]
        top1_column = runtime_variables["top1_column"]
        top2_column = runtime_variables["top2_column"]
        cell_total_column = runtime_variables["cell_total_column"]
        threshold = runtime_variables["threshold"]
        total_columns = runtime_variables["total_columns"]
        unique_identifier = runtime_variables["unique_identifier"]

        input_json = json.loads(runtime_variables["data"])

        input_dataframe = pd.DataFrame(input_json)
        stage_5_output = pd.DataFrame()
        first_loop = True
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
            if first_loop:
                stage_5_output = disclosure_output
                first_loop = False
            else:
                these_disclosure_columns = [this_disclosivity_marker,
                                            this_explanation,
                                            this_publishable_indicator]
                keep_columns = these_disclosure_columns + unique_identifier
                stage_5_output.drop(these_disclosure_columns, axis=1, inplace=True)
                stage_5_output = stage_5_output.merge(disclosure_output[keep_columns],
                                                      on=unique_identifier,
                                                      how="left")

            logger.info("Successfully completed Disclosure stage 5 for:"
                        + str(total_column))

        logger.info("Successfully completed Disclosure")

        final_output = {"data": stage_5_output.to_json(orient="records")}

    except Exception as e:
        error_message = general_functions.handle_exception(e, current_module,
                                                           run_id, context)
    finally:
        if (len(error_message)) > 0:
            logger.error(error_message)
            return {"success": False, "error": error_message}

    logger.info("Successfully completed module: " + current_module)
    final_output["success"] = True
    return final_output


def disclosure(input_df, disclosivity_marker, publishable_indicator,
               explanation, cell_total_column, top1_column, top2_column, threshold):
    """
    Takes in a dataframe and applies the stage5 disclosure rule.
    :param input_df: input data.
    :param disclosivity_marker: The name of the column to put "disclosive" marker.
    :param publishable_indicator: The name of the column to put "publish" marker.
    :param explanation: The name of the column to put reason for pass/fail.
    :param cell_total_column: The name of the column holding the cell total.
    :param top1_column: The name of the column largest contributor to the cell.
    :param top2_column: The name of the column second largest contributor to the cell.
    :param threshold: The threshold used in the disclosure calculation.
    :return output_df: Input dataframe with the addition of stage5 disclosure info.
    """
    def run_disclosure(row):
        if row[publishable_indicator] not in ("Publish", "No"):
            row["Score"] = \
                (row[cell_total_column] - row[top1_column] - row[top2_column]) \
                / row[top1_column]
            if row["Score"] >= float(threshold):
                row[disclosivity_marker] = "No"
                row[publishable_indicator] = "Publish"
                row[explanation] = "Stage 5 - Score is " + str(row["Score"])\
                                   + ". This meets threshold of (>=" + threshold\
                                   + ")"
            else:
                row[disclosivity_marker] = "Yes"
                row[publishable_indicator] = "No"
                row[explanation] = "Stage 5 - Score is " + str(row["Score"])\
                                   + ". This doesnt meet threshold of (>=" + threshold\
                                   + ")"
        return row

    output_df = input_df.apply(run_disclosure, axis=1)

    return output_df
