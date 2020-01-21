Custom Dataflow templates to read and load data to Datastore using Python 3.7.

Step 1:

Please clone the process_csv_json.py to your GCP cloud shell and run the command specified in " Execute DataFlow Python.txt". 
Once ran, you could able to see the template uploaded in GCS location specifed in template_location parameter.

Step 2:

Upload process_csv_json_metadata file in the same location of the template location.

Step 3:

Create a data flow selecting Custom Template and browse to your "process_csv_json"
