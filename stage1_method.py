import json
import logging

import pandas as pd
from es_aws_functions import general_functions, exception_classes
from marshmallow import EXCLUDE, Schema, fields


class RuntimeSchema(Schema):
    class Meta:
        unknown = EXCLUDE

    def handle_error(self, e, data, **kwargs):
        logging.error(f"Error validating runtime params: {e}")
        raise ValueError(f"Error validating runtime params: {e}")

    bpm_queue_url = fields.Str(required=True)
    disclosivity_marker = fields.Str(required=True)
    publishable_indicator = fields.Str(required=True)
    explanation = fields.Str(required=True)
    total_columns = fields.List(fields.Str(), required=True)
    cell_total_column = fields.Str(required=True)
    data = fields.Str(required=True)
    unique_identifier = fields.List(fields.Str(), required=True)
    run_id = fields.Str(required=True)
    environment = fields.Str(required=True)
    survey = fields.Str(required=True)


def lambda_handler(event, context):
    """
    Main entry point into method
    :param event: json payload containing:
            data: input data.
            bpm_queue_url: Queue url to send BPM status message.
            disclosivity_marker: The name of the column to put "disclosive" marker.
            publishable_indicator: The name of the column to put "publish" marker.
            explanation: The name of the column to put reason for pass/fail.
            total_columns: The names of the columns holding the cell totals.
            unique_identifier: The name of the column holding the contributor id.
            environment: The operating environment to use in the spp logger.
            survey: The survey selected to be used in the logger.
    :param context: AWS Context Object.
    :return final_output: Dict containing either:
            {"success": True, "data": <stage 1 output - json >}
            {"success": False, "error": <error message - string>}
    """
    current_module = "Disclosure Stage 1 Method"
    error_message = ""
    # Set-up variables for status message
    bpm_queue_url = None
    # Define run_id outside of try block
    run_id = 0
    try:
        # Retrieve run_id before input validation
        # Because it is used in exception handling
        run_id = event["RuntimeVariables"]["run_id"]
        runtime_variables = RuntimeSchema().load(event["RuntimeVariables"])

        # Runtime Variables
        bpm_queue_url = runtime_variables["bpm_queue_url"]
        disclosivity_marker = runtime_variables["disclosivity_marker"]
        publishable_indicator = runtime_variables["publishable_indicator"]
        explanation = runtime_variables["explanation"]
        unique_identifier = runtime_variables["unique_identifier"]
        total_columns = runtime_variables["total_columns"]
        cell_total_column = runtime_variables["cell_total_column"]
        environment = runtime_variables["environment"]
        survey = runtime_variables["survey"]
        input_json = json.loads(runtime_variables["data"])
    except Exception as e:
        error_message = general_functions.handle_exception(e, current_module,
                                                           run_id, context=context,
                                                           bpm_queue_url=bpm_queue_url)
        raise exception_classes.LambdaFailure(error_message)

    try:
        logger = general_functions.get_logger(survey, current_module, environment,
                                              run_id)
    except Exception as e:
        error_message = general_functions.handle_exception(e, current_module,
                                                           run_id, context=context,
                                                           bpm_queue_url=bpm_queue_url)
        raise exception_classes.LambdaFailure(error_message)

    try:
        logger.info("Started - retrieved wrangler configuration variables.")
        input_dataframe = pd.DataFrame(input_json)
        stage_1_output = pd.DataFrame()
        first_loop = True
        for total_column in total_columns:
            this_disclosivity_marker = disclosivity_marker + "_" + total_column
            this_publishable_indicator = publishable_indicator + "_" + total_column
            this_explanation = explanation + "_" + total_column
            this_total_column = cell_total_column + "_" + total_column

            disclosure_output = disclosure(input_dataframe,
                                           this_disclosivity_marker,
                                           this_publishable_indicator,
                                           this_explanation,
                                           this_total_column)
            if first_loop:
                stage_1_output = disclosure_output
                first_loop = False
            else:
                keep_columns = [this_disclosivity_marker,
                                this_explanation,
                                this_publishable_indicator] \
                               + unique_identifier
                stage_1_output = stage_1_output.merge(disclosure_output[keep_columns],
                                                      on=unique_identifier,
                                                      how="left")

            logger.info("Successfully completed Disclosure stage 1 for:"
                        + str(total_column))

        logger.info("Successfully completed Disclosure")
        final_output = {"data": stage_1_output.to_json(orient="records")}

    except Exception as e:
        error_message = general_functions.handle_exception(e, current_module,
                                                           run_id, context=context,
                                                           bpm_queue_url=bpm_queue_url)
    finally:
        if (len(error_message)) > 0:
            logger.error(error_message)
            return {"success": False, "error": error_message}

    logger.info("Successfully completed module.")
    final_output["success"] = True
    return final_output



    # current_module = "Disclosure Stage 1 Method"
    # error_message = ""
    # logger = general_functions.get_logger()
    # # Set-up variables for status message
    # bpm_queue_url = None
    # # Define run_id outside of try block
    # run_id = 0
    # try:
    #     # Retrieve run_id before input validation
    #     # Because it is used in exception handling
    #     run_id = event["RuntimeVariables"]["run_id"]
    #
    #     runtime_variables = RuntimeSchema().load(event["RuntimeVariables"])
    #
    #     logger.info("Validated parameters.")
    #
    #     # Runtime Variables
    #     bpm_queue_url = runtime_variables["bpm_queue_url"]
    #     disclosivity_marker = runtime_variables["disclosivity_marker"]
    #     publishable_indicator = runtime_variables["publishable_indicator"]
    #     explanation = runtime_variables["explanation"]
    #     unique_identifier = runtime_variables["unique_identifier"]
    #     total_columns = runtime_variables["total_columns"]
    #     cell_total_column = runtime_variables["cell_total_column"]
    #
    #     input_json = json.loads(runtime_variables["data"])
    #
    #     input_dataframe = pd.DataFrame(input_json)
    #     stage_1_output = pd.DataFrame()
    #     first_loop = True
    #     for total_column in total_columns:
    #         this_disclosivity_marker = disclosivity_marker + "_" + total_column
    #         this_publishable_indicator = publishable_indicator + "_" + total_column
    #         this_explanation = explanation + "_" + total_column
    #         this_total_column = cell_total_column + "_" + total_column
    #
    #         disclosure_output = disclosure(input_dataframe,
    #                                        this_disclosivity_marker,
    #                                        this_publishable_indicator,
    #                                        this_explanation,
    #                                        this_total_column)
    #         if first_loop:
    #             stage_1_output = disclosure_output
    #             first_loop = False
    #         else:
    #             keep_columns = [this_disclosivity_marker,
    #                             this_explanation,
    #                             this_publishable_indicator]\
    #                           + unique_identifier
    #             stage_1_output = stage_1_output.merge(disclosure_output[keep_columns],
    #                                                   on=unique_identifier,
    #                                                   how="left")
    #
    #         logger.info("Successfully completed Disclosure stage 1 for:"
    #                     + str(total_column))
    #
    #     logger.info("Successfully completed Disclosure")
    #     final_output = {"data": stage_1_output.to_json(orient="records")}
    #
    # except Exception as e:
    #     error_message = general_functions.handle_exception(e, current_module,
    #                                                        run_id, context=context,
    #                                                        bpm_queue_url=bpm_queue_url)
    # finally:
    #     if (len(error_message)) > 0:
    #         logger.error(error_message)
    #         return {"success": False, "error": error_message}
    #
    # logger.info("Successfully completed module: " + current_module)
    # final_output["success"] = True
    # return final_output


def disclosure(input_df, disclosivity_marker, publishable_indicator,
               explanation, total_column):
    """
    Takes in a dataframe and applies the stage1 disclosure rule.
    :param input_df: input data.
    :param disclosivity_marker: The name of the column to put "disclosive" marker.
    :param publishable_indicator: The name of the column to put "publish" marker.
    :param explanation: The name of the column to put reason for pass/fail.
    :param total_column - The name of the column to check.
    :return output_df: Input dataframe with the addition of stage1 disclosure info.
    """

    def run_disclosure(row):

        if row[total_column] == 0:
            row[disclosivity_marker] = "No"
            row[publishable_indicator] = "Publish"
            row[explanation] = "Stage 1 - Total column is 0"
        else:
            row[disclosivity_marker] = "Yes"
            row[publishable_indicator] = "Not Applicable"
            row[explanation] = "Through stage 1"
        return row

    output_df = input_df.apply(run_disclosure, axis=1)

    return output_df
