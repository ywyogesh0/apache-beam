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


# Sum - DoFn
class Sum(beam.DoFn):

    def process(self, element, *args, **kwargs):
        key, values = element
        return [(key, sum(values))]

    def to_runner_api_parameter(self, unused_context):
        pass


# format output
def format_output(element):
    emp_key_tuple, att_count = element
    dept_name, emp_id, emp_name = emp_key_tuple
    return ", ".join((dept_name, emp_id, emp_name, str(att_count)))


# Composite Transform - PTransform
class Count(beam.PTransform):

    def expand(self, input_or_inputs):
        avg_collection = (
                input_or_inputs

                | "Group by key" >>
                beam.GroupByKey()

                | "Sum" >>
                beam.ParDo(Sum())
        )

        return avg_collection


# Map unicode to ascii
def unicode_to_ascii(element):
    for index in range(len(element)):
        element[index] = str(element[index])
    return element


with beam.Pipeline() as attendance_count_pipeline:
    input_pcollection = (
            attendance_count_pipeline

            | "Read data from dept.txt to create PCollection Object" >>
            beam.io.ReadFromText("../resources/dept_data.txt")

            | "Split row on ',' delimiter" >>
            beam.ParDo(SplitRow())

            | "Map unicode to ascii" >>
            beam.Map(unicode_to_ascii)
    )

    accounts_pcollection = (
            input_pcollection

            | "Keep only 'Accounts' department rows" >>
            beam.ParDo(AccountsFilter())

            | "Create 'Accounts' Tuple (key, value) with key -> (Id, Name) and value -> 1" >>
            beam.ParDo(lambda filtered_row_arr: [(("Accounts", filtered_row_arr[0], filtered_row_arr[1]), 1)])

            | "'Accounts' -> Count" >> Count()
    )

    hr_pcollection = (
            input_pcollection

            | "Keep only 'HR' department rows" >>
            beam.ParDo(HRFilter())

            | "Create 'HR' Tuple (key, value) with key -> (Id, Name) and value -> 1" >>
            beam.ParDo(lambda filtered_row_arr: [(("HR", filtered_row_arr[0], filtered_row_arr[1]), 1)])

            | "'HR' -> Count" >> Count()
    )

    merged_pcollection = (
            (accounts_pcollection, hr_pcollection)

            | "Merge both PCollection object elements to create single unified PCollection object" >>
            beam.Flatten()

            | "Format output" >>
            beam.Map(format_output)

            | "Write each element of merged PCollection Object" >>
            beam.io.WriteToText(file_path_prefix="attendance_count_output/count",
                                header="dept_name, emp_id, emp_name, att_count")
    )
