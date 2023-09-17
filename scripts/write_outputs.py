import apache_beam as beam

p1 = beam.Pipeline()

voos = (
p1
  #ReadFromParquet
  #ReadFromBigQuery
  | "Importar Dados" >> beam.io.ReadFromText("/home/szot/projetos/eng/eng-gcp-apache-beam/files/voos_sample.csv", skip_header_lines = 1)
  | "Separar por Vírgul" >> beam.Map(lambda record: record.split(','))
  | "Gravar Resultado" >> beam.io.WriteToText("/home/szot/projetos/eng/eng-gcp-apache-beam/output/voos.txt")
)
p1.run()