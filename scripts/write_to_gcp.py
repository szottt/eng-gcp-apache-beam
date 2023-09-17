import apache_beam as beam
import os
import keys as chaves

#serviceAccount = chaves.serviceAccount
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = chaves.serviceAccount

p1 = beam.Pipeline()

class filtro(beam.DoFn):
    def process(self,record):
        if int(record[8]) > 0:
            return [record]

Tempo_atraso = (
  p1
  | "Importar Dados" >> beam.io.ReadFromText("/home/szot/projetos/eng/eng-gcp-apache-beam/files/voos_sample.csv", skip_header_lines = 1)
  | "Separar por Vírgulas" >> beam.Map(lambda record: record.split(','))
  | "Pegar voos Com atraso" >> beam.ParDo(filtro())
  | "Criar par" >> beam.Map(lambda record: (record[4], int(record[8])))
  | "Soma por key" >> beam.CombinePerKey(sum)
  #| "Mostrar Resultado" >> beam.Map(print)
)

Qtd_Atraso = (
  p1
  | "Importar Dados qtd" >> beam.io.ReadFromText("/home/szot/projetos/eng/eng-gcp-apache-beam/files/voos_sample.csv", skip_header_lines = 1)
  | "Separar por Vírgulas qtd" >> beam.Map(lambda record: record.split(','))
  | "Pegar qtd de voos com atraso" >> beam.ParDo(filtro())
  | "Criar par qtd" >> beam.Map(lambda record: (record[4], int(record[8])))
  | "Contar por key" >> beam.combiners.Count.PerKey()
  #| "Mostrar Resultado" >> beam.Map(print)
)


tabela_atrasos = (
    {'Qtd_Atrasos':Qtd_Atraso,'Tempo_atraso':Tempo_atraso}
    | "Group By" >> beam.CoGroupByKey()
    | "Saida Para GCP" >> beam.io.WriteToText(r"gs://apache-beam1/Voos_atrados_qtd2.csv")
)

p1.run()
