from abc import ABCMeta

import apache_beam as beam


class Combiner(beam.CombineFn):
    __metaclass__ = ABCMeta

    def create_accumulator(self, *args, **kwargs):
        return 0.0, 0

    def add_input(self, mutable_accumulator, element, *args, **kwargs):
        num, count = mutable_accumulator
        return num + element, count + 1

    def merge_accumulators(self, accumulators, *args, **kwargs):
        num_values, count_values = zip(*accumulators)
        return sum(num_values), sum(count_values)

    def extract_output(self, accumulator, *args, **kwargs):
        num_values, count_values = accumulator
        return num_values / count_values if count_values else float('NaN')


with beam.Pipeline() as combiner_pipeline:
    (
            combiner_pipeline

            | "Create list" >>
            beam.Create([1, 2, 3, 4, 5, 6])

            | "Using combiner" >>
            beam.CombineGlobally(Combiner())

            | "Writing output" >>
            beam.io.WriteToText("combiner_output/output")
    )
