import apache_beam as beam


# DoFn - Beam Class defining distributed processing function


# SplitRow - DoFn
class SplitRow(beam.DoFn):

    def process(self, element, *args, **kwargs):
        return [element.split(",")]

    def to_runner_api_parameter(self, unused_context):
        pass


# AccountsFilter - DoFn
class AccountsFilter(beam.DoFn):

    def process(self, element, *args, **kwargs):
        if element[3] == "Accounts":
            return [element]

    def to_runner_api_parameter(self, unused_context):
        pass


# HRFilter - DoFn
class HRFilter(beam.DoFn):

    def process(self, element, *args, **kwargs):
        if element[3] == "HR":
            return [element]

    def to_runner_api_parameter(self, unused_context):
        pass


# AccountsCount - DoFn
class AccountsCount(beam.DoFn):

    def process(self, element, *args, **kwargs):
        key, values = element
        return [(key, sum(values))]

    def to_runner_api_parameter(self, unused_context):
        pass


with beam.Pipeline() as attendance_count_pipeline:
    input_pcollection = (
            attendance_count_pipeline

            | "Read data from dept.txt to create PCollection Object" >>
            beam.io.ReadFromText("../resources/dept_data.txt")

            | "Split row on ',' delimiter" >>
            beam.ParDo(SplitRow())
    )

    accounts_pcollection = (
            input_pcollection

            | "Keep only 'Accounts' department rows" >>
            beam.ParDo(AccountsFilter())

            | "Create 'Accounts' Tuple (key, value) with key -> (Id, Name) and value -> 1" >>
            beam.ParDo(lambda filtered_row_arr: [(("Accounts", filtered_row_arr[0], filtered_row_arr[1]), 1)])

            | "'Accounts' -> Group by key" >>
            beam.GroupByKey()

            | "'Accounts' -> Count" >>
            beam.ParDo(AccountsCount())
    )

    hr_pcollection = (
            input_pcollection

            | "Keep only 'HR' department rows" >>
            beam.ParDo(HRFilter())

            | "Create 'HR' Tuple (key, value) with key -> (Id, Name) and value -> 1" >>
            beam.ParDo(lambda filtered_row_arr: [(("HR", filtered_row_arr[0], filtered_row_arr[1]), 1)])

            | "'HR' -> GroupByKey + Combiner (optimization) + Reducer (sum)" >>
            beam.CombinePerKey(sum)
    )

    merged_pcollection = (
            (accounts_pcollection, hr_pcollection)

            | "Merge both PCollection object elements to create single unified PCollection object" >>
            beam.Flatten()

            | "Write each element of merged PCollection Object" >>
            beam.io.WriteToText("attendance_count_output/count")
    )
