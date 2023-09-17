import apache_beam as beam

p1 = beam.Pipeline()

voos = (
p1
  | "Importar Dados" >> beam.io.ReadFromText("/home/szot/projetos/eng/eng-gcp-apache-beam/files/voos_sample.csv", skip_header_lines = 1)
  | "Separar por Vírgul" >> beam.Map(lambda record: record.split(','))
  | "Mostrar Resultado" >> beam.Map(print)
)
p1.run()


#ReadFromParquet
#ReadFromBigQuery