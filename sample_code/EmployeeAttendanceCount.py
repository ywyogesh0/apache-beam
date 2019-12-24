import apache_beam as beam

with beam.Pipeline() as attendance_count_pipeline:
    (
            attendance_count_pipeline

            | "Read data from dept.txt to create PCollection Object" >>
            beam.io.ReadFromText("../resources/dept_data.txt")

            | "Split row on ',' delimiter" >>
            beam.Map(lambda row: row.split(","))

            | "Keep only 'Accounts' department rows" >>
            beam.Filter(lambda row_arr: row_arr[3] == "Accounts")

            | "Create Tuple (key, value) with key -> (Id, Name) and value -> 1" >>
            beam.Map(lambda filtered_row_arr: ((filtered_row_arr[0], filtered_row_arr[1]), 1))

            | "GroupByKey + Combiner (optimization) + Reducer (sum)" >>
            beam.CombinePerKey(sum)

            | "Write each element of PCollection Object" >>
            beam.io.WriteToText("attendance_count_output/count")
    )
