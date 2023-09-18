import apache_beam as beam
import os
import keys as keys
from apache_beam.options.pipeline_options import PipelineOptions

#serviceAccount = chaves.serviceAccount
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = keys.serviceAccount

pipeline_options = {
    'project': 'curso-dataflow-beam-399318' ,
    'runner': 'DataflowRunner',
    'region': 'southamerica-east1',
    'staging_location': 'gs://apache-beam1/temp',
    'temp_location': 'gs://apache-beam1/temp',
    'template_location': 'gs://apache-beam1/template/batch_job_df_gcs_voos'
    }

pipeline_options = PipelineOptions.from_dictionary(pipeline_options)
p1 = beam.Pipeline(options=pipeline_options)

input = 'gs://apache-beam1/input/voos_sample.csv'

class filtro(beam.DoFn):
    def process(self,record):
        if int(record[8]) > 0:
            return [record]

Tempo_atraso = (
  p1
  | "Importar Dados" >> beam.io.ReadFromText(input, skip_header_lines = 1)
  | "Separar por Vírgulas" >> beam.Map(lambda record: record.split(','))
  | "Pegar voos Com atraso" >> beam.ParDo(filtro())
  | "Criar par" >> beam.Map(lambda record: (record[4], int(record[8])))
  | "Soma por key" >> beam.CombinePerKey(sum)
  #| "Mostrar Resultado" >> beam.Map(print)
)

Qtd_Atraso = (
  p1
  | "Importar Dados qtd" >> beam.io.ReadFromText(input, skip_header_lines = 1)
  | "Separar por Vírgulas qtd" >> beam.Map(lambda record: record.split(','))
  | "Pegar qtd de voos com atraso" >> beam.ParDo(filtro())
  | "Criar par qtd" >> beam.Map(lambda record: (record[4], int(record[8])))
  | "Contar por key" >> beam.combiners.Count.PerKey()
  #| "Mostrar Resultado" >> beam.Map(print)
)


tabela_atrasos = (
    {'Qtd_Atrasos':Qtd_Atraso,'Tempo_atraso':Tempo_atraso}
    | "Group By" >> beam.CoGroupByKey()
    | "Saida Para GCP" >> beam.io.WriteToText(r"gs://apache-beam1/output/Voos_atrados_qtd2.csv")
)

p1.run()
