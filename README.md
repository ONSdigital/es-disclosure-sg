# es-disclosure
The disclosure module allows the user to specify the steps which they wish to run against the data. e.g. 1, 2, 5. Currently Stages 1, 2, 5 are implemented but 3 & 4 are currently in development and are present as mocks.

The disclosure methods relies on their being several aggregations produced by the previous step. Refer to aggregation for more information.
Mike
## Wrangler
Disclosure utilises a single wrangler to orchestrated which method stages are triggered. This is specified via the disclosure_stages runtime variable.

### Common Environment Variables
Each wrangler has these variables:<br>
checkpoint:    - Used by step function for starting part way through the process.(default of 5)<br>
bucket_name:   - The name of the bucket used to store data.<br>
method_name:   - The method that this wrangler calls.<br>
sns_topic_arn: - The sns topic to send summary information to.<br>
sqs_queue_url: - The sqs queue url to use in sending/receiving sqs messages.<br>

### Runtime variables
These are the runtime variables that need to be present for the module to work correctly.<br>
disclosivity_marker: -  Marks if the data is disclosive or not.<br>
publishable_indicator: - Marks if the data should be published or not.<br>
explanation: - The reason why something has been marked as disclosive.<br>
total_column: - The name which is used for the total column in aggregation.<br>
parent_column: - The name of the reference of the parent company<br>
threshold: - The threshold used in the calculation of one of the disclosure calculations.<br>
cell_total_column: - The name given to the cell total column.<br>
top1_column: - The name of the column that holds the largest contributor cell.<br>
top2_column: - The name of the column that holds the second largest contributor cell.<br>
stage5_threshold: - The threshold used in the calculation of one of the disclosure calculations.<br>
disclosure_stages: - The stages of disclosure you wish to run e.g. 1, 2, 5.<br>
queue_url: - The sqs queue url to use in sending/receiving sqs messages.<br>
in_file_name:  - The default input file name to get from s3 (this is the previous methods out_file_name).<br>
incoming_message_group_id: - The message group that this wranglers input message will arrive from (this is the previous methods outgoing_message_group_id).<br>
out_file_name: - The path and name of the file you wish to save the csv as.<br>
outgoing_message_group_id: - The message group this wrangler will attach to its output message.<Br>

### General process: <br>
- Collect the data from sqs <br>
- Turn input data into dataframe <br>
- Pass input dataframe to the appropriate method <br>
- Send returned data from method to sqs queue <br>
- Delete input message from sqs <br>
- Send summary info to sns. <br>
<br>

## Methods
The methods perform the actual disclosure calculation. Each contains a method called 
disclosure which contains an apply() method to apply a given test to each row of the 
dataframe. Once applied, the dataframe is returned.

### Stage 1

**Name of Lambda:**

stage1_method

**Intro:**

Checks whether the total for the cell is 0 or rounded to 0

**Inputs:**

json_data: input data.                                                   <Br>
disclosivity_marker: The name of the column to put 'disclosive' marker. <Br>
publishable_indicator: The name of the column to put 'publish' marker.    <Br>
explanation: The name of the column to put reason for pass/fail.<Br>
cell_total_column: The name of the column holding the cell total.<Br>
total_columns: The names of the columns holding the cell totals.<Br>
contributor_reference: The name of the column holding the contributor id.            

**Outputs:**

final_output: Dict containing either:<br>
            {"success": True, "data": < stage 1 output - json >}<br>
            {"success": False, "error": < error message - string >}<br>

### Stage 2

**Name of Lambda:**

stage2_method

**Intro:**

Checks whether the number of different ent refs in a cell is at least as much as a 
certain threshold.

**Inputs:**

json_data: input data.                                                    <Br>
disclosivity_marker: The name of the column to put 'disclosive' marker.  <Br>
publishable_indicator: The name of the column to put 'publish' marker.     <Br>
explanation: The name of the column to put reason for pass/fail. <Br>
parent_column: The name of the column holding the count of parent company.<Br>
threshold: The threshold above which a row is not disclosive.  <Br>
total_columns: The names of the column holding the cell totals. Included so that correct disclosure columns used.<Br>
contributor_reference: The name of the column holding the contributor id.     <Br>    

**Outputs:**

final_output: Dict containing either:<br>
            {"success": True, "data": < stage 2 output - json >}<br>
            {"success": False, "error": < error message - string >}<br>

### Stage 3
N/A

### Stage 4
N/A

### Stage 5

**Name of Lambda:**

stage5_method

**Intro:**

Not including the specifics of this test as it may be considered sensitive

**Inputs:**

json_data: input data.                                                      
disclosivity_marker: The name of the column to put 'disclosive' marker.    <Br>
publishable_indicator: The name of the column to put 'publish' marker.      <Br> 
explanation: The name of the column to put reason for pass/fail.   <Br>
cell_total_column: The name of the column holding the cell total.               
top1_column: The name of the column largest contributor to the cell.<Br>
top2_column: The name of the column second largest contributor to the cell.    <Br>
total_columns: The names of the columns holding the cell totals. Included so that correct disclosure columns used.<Br>
contributor_reference: The name of the column holding the contributor id.<Br>
            
**Outputs:**

final_output: Dict containing either:<br>
            {"success": True, "data": < stage 5 output - json >}<br>
            {"success": False, "error": < error message - string >}<br>
