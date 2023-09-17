import apache_beam as beam

p2 = beam.Pipeline()

Collection = (
    p2
    | "Importar Dados" >> beam.io.ReadFromText('/home/szot/projetos/eng/eng-gcp-apache-beam/files/poema.txt')
    | "Separando por Virgulas" >> beam.FlatMap(lambda record: record.split(' '))
    | "Gravando o Resultado" >> beam.io.WriteToText('/home/szot/projetos/eng/eng-gcp-apache-beam/output/resultado.txt')
)

p2.run()