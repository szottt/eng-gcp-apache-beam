import apache_beam as beam

p1 = beam.Pipeline()

voos = (
p1
  #ReadFromParquet
  #ReadFromBigQuery
  | "Importar Dados" >> beam.io.ReadFromText("/home/szot/projetos/eng/eng-gcp-apache-beam/files/voos_sample.csv", skip_header_lines = 1)
  | "Separar por Vírgulas" >> beam.Map(lambda record: record.split(','))
  | "Pegar voos de Los Angeles" >> beam.Filter(lambda record: int(record[8]) > 0 )
  | "Criar par" >> beam.Map(lambda record: (record[4], int(record[8])))
  | "Soma por key" >> beam.CombinePerKey(sum)
  | "Mostrar Resultado" >> beam.Map(print)
  #| "Gravar Resultado" >> beam.io.WriteToText("voos.txt")
)
p1.run()