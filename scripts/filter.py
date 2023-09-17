import apache_beam as beam

p1 = beam.Pipeline()

voos = (
p1
  | "Importar Dados" >> beam.io.ReadFromText("/home/szot/projetos/eng/eng-gcp-apache-beam/files/voos_sample.csv", skip_header_lines = 1)
  | "Separar por Vírgul" >> beam.Map(lambda record: record.split(','))
  | "Pegar voos de Los Angeles" >> beam.Filter(lambda record: record[3] == 'LAX')
  | "Mostrar Resultado" >> beam.Map(print)
  #| "Gravar Resultado" >> beam.io.WriteToText("voos.txt")
)
#p1.run()


palavras=['quatro','um']

def encontrarPlavras( i ):
  if i in palavras:
    return True

p2_3 = beam.Pipeline()

Collection = (
    p2_3
    | "Importar Dados" >> beam.io.ReadFromText('/home/szot/projetos/eng/eng-gcp-apache-beam/files/poema.txt')
    | "Separando por Virgulas" >> beam.FlatMap(lambda record: record.split(' '))
    | "Filtrando" >> beam.Filter(encontrarPlavras)
    | "Mostrar Resultado" >> beam.Map(print)
    #| "Gravando o Resultado" >> beam.io.WriteToText('resultado.txt')
)

p2_3.run()