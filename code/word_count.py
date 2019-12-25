import apache_beam as beam
import re

input_path = "../resources/*"
output_path = "word_count_output/word_count"

find_all_regex = r"[a-zA-Z']+"

# Running locally in the DirectRunner
with beam.Pipeline() as data_pipeline:
    (
            data_pipeline

            | "Read each and every file" >>
            beam.io.ReadFromText(input_path)

            | "Split on whitespace characters by using regex" >>
            beam.FlatMap(lambda row: re.findall(find_all_regex, row))

            | "Create word tuple -> (word, 1)" >>
            beam.Map(lambda word: (word, 1))

            | "GroupByKey + Combiner + Reducer" >>
            beam.CombinePerKey(sum)

            | "Format result" >>
            beam.Map(lambda result: str(result))

            | "Write each element of PCollection" >>
            beam.io.WriteToText(output_path)
    )
