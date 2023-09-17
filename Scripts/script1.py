import apache_beam as beam

# p1 = beam.Pipeline()

# voos = (
# p1
#   #ReadFromParquet
#   #ReadFromBigQuery
#   | "Importar Dados" >> beam.io.ReadFromText("/home/szot/projetos/eng/gcp/files/voos_sample.csv", skip_header_lines = 1)
#   | "Separar por Vírgul" >> beam.Map(lambda record: record.split(','))
#   #| "Mostrar Resultado" >> beam.Map(print)
#   | "Gravar Resultado" >> beam.io.WriteToText("voos.txt")
# )
# p1.run()

# p2 = beam.Pipeline()

# Collection = (
#     p2
#     | "Importar Dados" >> beam.io.ReadFromText('/home/szot/projetos/eng/gcp/files/poema.txt')
#     | "Separando por Virgulas" >> beam.FlatMap(lambda record: record.split(' '))
#     | "Gravando o Resultado" >> beam.io.WriteToText('resultado.txt')
# )

# p2.run()

# palavras=['quatro','um']

# def encontrarPlavras( i ):
#   if i in palavras:
#     return True

# p2_3 = beam.Pipeline()

# Collection = (
#     p2_3
#     | "Importar Dados" >> beam.io.ReadFromText('/home/szot/projetos/eng/gcp/files/poema.txt')
#     | "Separando por Virgulas" >> beam.FlatMap(lambda record: record.split(' '))
#     | "Filtrando" >> beam.Filter(encontrarPlavras)
#     | "Mostrar Resultado" >> beam.Map(print)
#     #| "Gravando o Resultado" >> beam.io.WriteToText('resultado.txt')
# )

# p2_3.run()

# p3 = beam.Pipeline()

# voos = (
# p3
#   #ReadFromParquet
#   #ReadFromBigQuery
#   | "Importar Dados" >> beam.io.ReadFromText("/home/szot/projetos/eng/gcp/files/voos_sample.csv", skip_header_lines = 1)
#   | "Separar por Vírgul" >> beam.Map(lambda record: record.split(','))
#   | "Pegar voos de Los Angeles" >> beam.Filter(lambda record: record[3] == 'LAX')
#   | "Mostrar Resultado" >> beam.Map(print)
#   #| "Gravar Resultado" >> beam.io.WriteToText("voos.txt")
# )
# p3.run()


# p3 = beam.Pipeline()

# voos = (
# p3
#   | "Importar Dados" >> beam.io.ReadFromText("/home/szot/projetos/eng/gcp/files/voos_sample.csv", skip_header_lines = 1)
#   | "Separar por Vírgul" >> beam.Map(lambda record: record.split(','))
#   | "Pegar voos com atraso" >> beam.Filter(lambda record: int(record[8]) > 0)
#   | "Criar par" >> beam.Map(lambda record: (record[4], int(record[8])))
#   | "Soma por Key" >> beam.CombinePerKey(sum)
#   | "Mostrar Resultado" >> beam.Map(print)
# )
# p3.run()

# p = beam.Pipeline()

# negros = ('Adão','Jesus','Mike')
# brancos = ('Tulio','Mary ','Joca')
# indios = ('Vic','Marta','Tom')

# negros_pc = p | "Criando Pcollection negros" >> beam.Create(negros)
# brancos_pc = p | "Criando Pcollection brancos" >> beam.Create(brancos)
# indios_pc = p | "Criando Pcollection indios" >> beam.Create(indios)

# pessoas = ((negros_pc, brancos_pc, indios_pc) | beam.Flatten()) | beam.Map(print)

# p.run()
#/home/szot/projetos/eng/gcp/files//home/szot/projetos/eng/gcp/files/voos_sample.csv

#CombinePerKey ==================================================================================================================================================

# p3 = beam.Pipeline()

# voos = (
# p3
#   #ReadFromParquet
#   #ReadFromBigQuery
#   | "Importar Dados" >> beam.io.ReadFromText("/home/szot/projetos/eng/gcp/files/voos_sample.csv", skip_header_lines = 1)
#   | "Separar por Vírgulas" >> beam.Map(lambda record: record.split(','))
#   | "Pegar voos de Los Angeles" >> beam.Filter(lambda record: int(record[8]) > 0 )
#   | "Criar par" >> beam.Map(lambda record: (record[4], int(record[8])))
#   | "Soma por key" >> beam.CombinePerKey(sum)
#   | "Mostrar Resultado" >> beam.Map(print)
#   #| "Gravar Resultado" >> beam.io.WriteToText("voos.txt")
# )
# p3.run()

#CombinePerKey ==================================================================================================================================================

# p3 = beam.Pipeline()

# voos = (
# p3
#   #ReadFromParquet
#   #ReadFromBigQuery
#   | "Importar Dados" >> beam.io.ReadFromText("/home/szot/projetos/eng/gcp/files/voos_sample.csv", skip_header_lines = 1)
#   | "Separar por Vírgulas" >> beam.Map(lambda record: record.split(','))
#   | "Pegar voos de Los Angeles" >> beam.Filter(lambda record: int(record[8]) > 0 )
#   | "Criar par" >> beam.Map(lambda record: (record[4], int(record[8])))
#   | "Contar por key" >> beam.combiners.Count.PerKey()
#   | "Mostrar Resultado" >> beam.Map(print)
#   #| "Gravar Resultado" >> beam.io.WriteToText("voos.txt")
# )
# p3.run()

#GroupByKey ==================================================================================================================================================

p1 = beam.Pipeline()

Tempo_atraso = (
  p1
  | "Importar Dados" >> beam.io.ReadFromText("/home/szot/projetos/eng/gcp/files/voos_sample.csv", skip_header_lines = 1)
  | "Separar por Vírgulas" >> beam.Map(lambda record: record.split(','))
  | "Pegar voos de Los Angeles" >> beam.Filter(lambda record: int(record[8]) > 0 )
  | "Criar par" >> beam.Map(lambda record: (record[4], int(record[8])))
  | "Soma por key" >> beam.CombinePerKey(sum)
  #| "Mostrar Resultado" >> beam.Map(print)
)

Qtd_Atraso = (
  p1
  | "Importar Dados qtd" >> beam.io.ReadFromText("/home/szot/projetos/eng/gcp/files/voos_sample.csv", skip_header_lines = 1)
  | "Separar por Vírgulas qtd" >> beam.Map(lambda record: record.split(','))
  | "Pegar voos de Los Angeles qtd" >> beam.Filter(lambda record: int(record[8]) > 0 )
  | "Criar par qtd" >> beam.Map(lambda record: (record[4], int(record[8])))
  | "Contar por key" >> beam.combiners.Count.PerKey()
  #| "Mostrar Resultado" >> beam.Map(print)
)

tabela_Atraso = (
    {'Qtd_Atraso':Qtd_Atraso,'Tempo_atraso':Tempo_atraso}
    | "GroupBy" >> beam.CoGroupByKey()
    | beam.Map(print)
)

p1.run()
