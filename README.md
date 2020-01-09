# es-disclosure
The disclosure methods for surveys. This is merely applying 3 of 5 disclosure stages to the data to determine whether it 
is publishable or not.

The disclosure methods rely on their being several aggregations produced by the 
previous step.
## Wranglers
The wranglers for disclosure are virtually the same code for each. Things that will change in aws are some environment variables, and the runtime variables sent in. This just needs to point to the appropriate 
 method(eg, stage 1 uses the stage1_method, etc)

### Common Environment Variables
Each wrangler has these variables:<br>
checkpoint:    - Used by step function for starting part way through the process.(default of 5)<br>
bucket_name:   - The name of the bucket used to store data.<br>
in_file_name:  - The default input file name to get from s3 (this is the previous methods out_file_name).<br>
incoming_message_group: - The message group that this wranglers input message will arrive from (this is the previous methods sqs_message_group_id).<br>
method_name:   - The method that this wrangler calls.<br>
out_file_name: - The filename this wrangler uses to save its output.<br>
sns_topic_arn: - The sns topic to send summary information to.<br>
sqs_message_group_id: - The message group this wrangler will attach to its output message.<Br>
sqs_queue_url: - The sqs queue url to use in sending/receiving sqs messages.<br>

### General process: <br>
- Collect the data from sqs <br>
- Turn input data into dataframe <br>
- Pass input dataframe to the appropriate method <br>
- Send returned data from method to sqs queue <br>
- Delete input message from sqs <br>
- Send summary info to sns. <br>
<br>

### Stage 1

Additionally creates the Disclosive, Publish, and Reason columns to hold the results of
disclosure.
 
- Disclosive - Indicates whether the data is disclosive or not<br>
- Publish - Can this data be published<br>
- Reason - If data can/cannot be published, this is why.<br>

#### Runtime Variables Required:<br>
disclosivity_marker: The name of the column to put 'disclosive' marker. <br>
publishable_indicator: The name of the column to put 'publish' marker. <br>
explanation: The name of the column to put reason for pass/fail. <br>
total_column: The name of the column holding the cell total. <br>

### Stage 2

#### Runtime Variables Required:<br>
disclosivity_marker: The name of the column to put 'disclosive' marker. <br>
publishable_indicator: The name of the column to put 'publish' marker. <br>
explanation: The name of the column to put reason for pass/fail. <br>
parent_column: The name of the column holding the count of parent company.<br>
threshold: The threshold above which a row is not disclosive.<br>

### Stage 5

#### Runtime Variables Required:<br>
disclosivity_marker: The name of the column to put 'disclosive' marker. <br>
publishable_indicator: The name of the column to put 'publish' marker. <br>
explanation: The name of the column to put reason for pass/fail. <br>
total_column: The name of the column holding the cell total. <br>
top1_column: The name of the column largest contributor to the cell. <br>            
top2_column: The name of the column second largest contributor to the cell.  <br>    

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

json_data: input data.                                                   
disclosivity_marker: The name of the column to put 'disclosive' marker. 
publishable_indicator: The name of the column to put 'publish' marker.    
explanation: The name of the column to put reason for pass/fail.
cell_total_column: The name of the column holding the cell total.            

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

json_data: input data.                                                    
disclosivity_marker: The name of the column to put 'disclosive' marker.  
publishable_indicator: The name of the column to put 'publish' marker.     
explanation: The name of the column to put reason for pass/fail. 
parent_column: The name of the column holding the count of parent company
threshold: The threshold above which a row is not disclosive.            

**Outputs:**

final_output: Dict containing either:<br>
            {"success": True, "data": < stage 2 output - json >}<br>
            {"success": False, "error": < error message - string >}<br>

### Stage 5

**Name of Lambda:**

stage5_method

**Intro:**

Not including the specifics of this test as it may be considered sensitive

**Inputs:**

json_data: input data.                                                      
disclosivity_marker: The name of the column to put 'disclosive' marker.    
publishable_indicator: The name of the column to put 'publish' marker.       
explanation: The name of the column to put reason for pass/fail.   
cell_total_column: The name of the column holding the cell total.               
top1_column: The name of the column largest contributor to the cell.
top2_column: The name of the column second largest contributor to the cell.    

**Outputs:**

final_output: Dict containing either:<br>
            {"success": True, "data": < stage 5 output - json >}<br>
            {"success": False, "error": < error message - string >}<br>
