from pyspark.sql.dataframe import DataFrame


def assert_dataframe_equals(expected_df: DataFrame, output_df: DataFrame):
    """
    Produced Data frames are not garanteed to be with the same column and row order as expected one
    To compare them effenciently we start by reordering columns and sorting rows
    """
    expected_col_set = set(expected_df.columns)
    output_col_set = set(output_df.columns)
    if expected_col_set != output_col_set:
        detailed_error_msg = f"""
        Columns are different than expected


        Actual columns:
        

        {sorted(output_df.columns)}
        
        
        Expected columns:
        

        {sorted(expected_df.columns)}
        
        

        """
        assert False, detailed_error_msg
    expected_columns = [f"`{col}`" for col in expected_df.columns]
    sorted_expected_df = expected_df.sort(*expected_columns)
    sorted_output_df = output_df.select(*expected_columns).sort(*expected_columns)
    sorted_expected_list = sorted_expected_df.collect()
    sorted_output_list = sorted_output_df.collect()
    if sorted_expected_list != sorted_output_list:
        output_display = '\n        '.join([str(row) for row in sorted_output_list])
        expected_display = '\n      '.join([str(row) for row in sorted_expected_list])
        detailed_error_msg = f"""
        Dataframe is different than expected
        
        Actual dataframe:

        {output_display}
        
        Expected dataframe:

        {expected_display}
        """
        print(detailed_error_msg)
        #short message for assertion message (due to the limitation of display in the test report)
        output_diff_sample = [sorted_output_list[i] for i in range(0, min(len(sorted_output_list), len(sorted_expected_list))) if sorted_output_list[i] != sorted_expected_list[i]][:10]
        expected_diff_sample = [sorted_expected_list[i] for i in range(0, min(len(sorted_output_list), len(sorted_expected_list))) if sorted_output_list[i] != sorted_expected_list[i]][:10]
        output_diff_display = '\n        '.join([str(row) for row in output_diff_sample])
        expected_diff_display = '\n      '.join([str(row) for row in expected_diff_sample])
        diff_sample_error_msg = f"""
        Dataframe is different than expected
        
        Actual dataframe diff sample:

        {output_diff_display}
        
        Expected dataframe diff sample:

        {expected_diff_display}
        """
        assert False, diff_sample_error_msg