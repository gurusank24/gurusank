import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions, SetupOptions
import logging
from google.cloud import datastore
from apache_beam.options.value_provider import RuntimeValueProvider

class ConvertToJson(beam.DoFn):
    def process(self, element):
        import json
        return json.loads("[" + element + "]")

class CreateHbaseRow(beam.DoFn): 
    def __init__(self, project_id):
       self.project_id = project_id

    def start_bundle(self):
        self.client = datastore.Client()

    def process(self, element):
        try:
            key = self.client.key( user_options.datastore_key.get() ,element[user_options.datastore_id.get()])
            entity = datastore.Entity(key=key)
            entity.update(element)  
            self.client.put(entity) 
        except:   
            logging.error("Failed with input: ", str(element))

class MyOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
                '--json_input',
                dest='json_input',
                type=str,
                required=False,
                help='Input file to read. This can be a local file or a file in a Google Storage Bucket.')
        parser.add_value_provider_argument(
                '--project_id',
                dest='project_id',
                type=str,
                required=False,
                help='Input Project ID.')
        parser.add_value_provider_argument(
                '--datastore_key',
                dest='datastore_key',
                type=str,
                required=False,
                help='The Key name')

        parser.add_value_provider_argument(
                '--datastore_id',
                dest='datastore_id',
                type=str,
                required=False,
                help='Proive the unique Datastore ID to load ')
                
options = PipelineOptions(save_main_session=True)

user_options = options.view_as(MyOptions)

google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.job_name = 'gcstofirestore'
options.view_as(StandardOptions).runner = 'DataflowRunner'

p = beam.Pipeline(options=options)

lines_text  = p | "Read Json From GCS" >> beam.io.ReadFromText(user_options.json_input)
lines_json = lines_text | "Convert To Json" >> beam.ParDo(ConvertToJson()) 
lines_json | "Create Entities From Json" >> beam.ParDo(CreateHbaseRow(user_options.project_id))
p.run().wait_until_finish()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.ERROR)