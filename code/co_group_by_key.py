import apache_beam as beam


def formatted_key_value_tuple(row):
    row_arr = row.split(",")

    for index in range(len(row_arr)):
        row_arr[index] = str(row_arr[index])

    return row_arr[0], row_arr[1:]


with beam.Pipeline() as co_group_by_key_pipeline:
    dept_col = (
            co_group_by_key_pipeline

            | "Read dept_data.txt" >>
            beam.io.ReadFromText("../resources/dept_data.txt")

            | "dept_col -> Create formatted (key, value) tuple" >>
            beam.Map(formatted_key_value_tuple)
    )

    loc_col = (
            co_group_by_key_pipeline

            | "Read location.txt" >>
            beam.io.ReadFromText("../resources/dept_data.txt")

            | "loc_col -> Create formatted (key, value) tuple" >>
            beam.Map(formatted_key_value_tuple)
    )

    (
            {'department_collection': dept_col, 'location_collection': loc_col}

            | "CoGroupByKey" >>
            beam.CoGroupByKey()

            | "Write output" >>
            beam.io.WriteToText("co_group_by_key/output")
    )
