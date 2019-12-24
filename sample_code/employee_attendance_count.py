import apache_beam as beam

with beam.Pipeline() as attendance_count_pipeline:
    input_pcollection = (
            attendance_count_pipeline

            | "Read data from dept.txt to create PCollection Object" >>
            beam.io.ReadFromText("../resources/dept_data.txt")

            | "Split row on ',' delimiter" >>
            beam.Map(lambda row: row.split(","))
    )

    accounts_pcollection = (
            input_pcollection

            | "Keep only 'Accounts' department rows" >>
            beam.Filter(lambda row_arr: row_arr[3] == "Accounts")

            | "Create 'Accounts' Tuple (key, value) with key -> (Id, Name) and value -> 1" >>
            beam.Map(lambda filtered_row_arr: (("Accounts", filtered_row_arr[0], filtered_row_arr[1]), 1))

            | "'Accounts' -> GroupByKey + Combiner (optimization) + Reducer (sum)" >>
            beam.CombinePerKey(sum)
    )

    hr_pcollection = (
            input_pcollection

            | "Keep only 'HR' department rows" >>
            beam.Filter(lambda row_arr: row_arr[3] == "HR")

            | "Create 'HR' Tuple (key, value) with key -> (Id, Name) and value -> 1" >>
            beam.Map(lambda filtered_row_arr: (("HR", filtered_row_arr[0], filtered_row_arr[1]), 1))

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
