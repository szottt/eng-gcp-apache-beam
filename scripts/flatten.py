import apache_beam as beam

p1 = beam.Pipeline()

negros = ('Adão','Jesus','Mike')
brancos = ('Tulio','Mary ','Joca')
indios = ('Vic','Marta','Tom')

negros_pc = p1 | "Criando Pcollection negros" >> beam.Create(negros)
brancos_pc = p1 | "Criando Pcollection brancos" >> beam.Create(brancos)
indios_pc = p1 | "Criando Pcollection indios" >> beam.Create(indios)

pessoas = ((negros_pc, brancos_pc, indios_pc) | beam.Flatten()) | beam.Map(print)

p1.run()