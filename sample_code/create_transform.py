import apache_beam as beam

list_pipeline = beam.Pipeline()
set_pipeline = beam.Pipeline()
dict_pipeline = beam.Pipeline()

num_list = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
names_set = ('Yogesh', 'Walia')
num_dict = {
    'even': [2, 4, 6, 8, 10],
    'odd': [1, 3, 5, 7, 9]
}

# apache_beam.pipeline.Pipeline
# apache_beam.pvalue.PCollection

(
        list_pipeline
        | beam.Create(num_list)
        | beam.io.WriteToText('output/list')
)

(
        set_pipeline
        | beam.Create(names_set)
        | beam.io.WriteToText('output/set')
)

(
        dict_pipeline
        | beam.Create(num_dict)
        | beam.io.WriteToText('output/dict')
)

list_pipeline.run()
set_pipeline.run()
dict_pipeline.run()
