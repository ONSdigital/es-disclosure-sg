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
    parent_column = fields.Str(required=True)
    threshold = fields.Str(required=True)
    data = fields.Str(required=True)
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
            parent_column: The name of the column holding the count of parent company.
            threshold: The threshold above which a row is not disclosive.
            total_columns: The names of the column holding the cell totals.
                        Included so that correct disclosure columns used.
            unique_identifier: The name of the column holding the contributor id.
    :param context: AWS Context Object.
    :return final_output: Dict containing either:
            {"success": True, "data": <stage 2 output - json >}
            {"success": False, "error": <error message - string>}
    """
    current_module = "Disclosure Stage 2 Method"
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
        parent_column = runtime_variables["parent_column"]
        threshold = int(runtime_variables["threshold"])
        total_columns = runtime_variables["total_columns"]
        unique_identifier = runtime_variables["unique_identifier"]

        input_json = json.loads(runtime_variables["data"])

        input_dataframe = pd.DataFrame(input_json)
        stage_2_output = pd.DataFrame()
        first_loop = True
        for total_column in total_columns:
            this_disclosivity_marker = disclosivity_marker + "_" + total_column
            this_publishable_indicator = publishable_indicator + "_" + total_column
            this_explanation = explanation + "_" + total_column

            disclosure_output = disclosure(input_dataframe,
                                           this_disclosivity_marker,
                                           this_publishable_indicator,
                                           this_explanation,
                                           parent_column,
                                           threshold)
            if first_loop:
                stage_2_output = disclosure_output
                first_loop = False
            else:
                these_disclosure_columns = [this_disclosivity_marker,
                                            this_explanation,
                                            this_publishable_indicator]
                keep_columns = these_disclosure_columns + unique_identifier
                stage_2_output.drop(these_disclosure_columns, axis=1, inplace=True)
                stage_2_output = stage_2_output.merge(disclosure_output[keep_columns],
                                                      on=unique_identifier,
                                                      how="left")

            logger.info("Successfully completed Disclosure stage 2 for:"
                        + str(total_column))

        logger.info("Successfully completed Disclosure")
        final_output = {"data": stage_2_output.to_json(orient="records")}

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
               explanation, parent_column, threshold):
    """
    Takes in a dataframe and applies the stage2 disclosure rule.
    :param input_df: input data.
    :param disclosivity_marker: The name of the column to put "disclosive" marker.
    :param publishable_indicator: The name of the column to put "publish" marker.
    :param explanation: The name of the column to put reason for pass/fail.
    :param parent_column: The name of the column holding the count of parent company.
    :param threshold: The threshold above which a row is not disclosive.
    :return output_df: Input dataframe with the addition of stage2 disclosure info.
    """
    def run_disclosure(row):
        if row[publishable_indicator] != "Publish":
            if row[parent_column] < float(threshold):
                row[disclosivity_marker] = "Yes"
                row[publishable_indicator] = "No"
                row[explanation] = "Stage 2 - Only "\
                                   + str(row[parent_column])\
                                   + " parent references in cell"
            else:
                row[disclosivity_marker] = "No"
                row[publishable_indicator] = "Not Applicable"
                row[explanation] = "Passed Stage 2"
        return row

    output_df = input_df.apply(run_disclosure, axis=1)

    return output_df
