import great_expectations as ge

context = ge.get_context()
validator = context.sources.pandas_default.read_csv("yellow_taxi.csv")
validator.expect_column_values_to_not_be_null("pickup_datetime")
validator.expect_column_values_to_be_between("passenger_count", 10, 20)
validator.save_expectation_suite(discard_failed_expectations=False)

context.add_or_update_checkpoint(
    name="my_quickstart_checkpoint",
    validator=validator,
)
checkpoint_result = context.run_checkpoint(checkpoint_name="my_quickstart_checkpoint")
stats = checkpoint_result.get_statistics()

print(next(iter(stats["validation_statistics"].values())))
# print(validator.validate().statistics) # does the same
