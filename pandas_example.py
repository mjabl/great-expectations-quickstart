import great_expectations as ge

# get context
context = ge.get_context()

# configure validator
validator = context.sources.pandas_default.read_csv("yellow_taxi.csv")
validator.expect_column_values_to_not_be_null("pickup_datetime")
validator.expect_column_values_to_be_between("passenger_count", 10, 20)
validator.save_expectation_suite(discard_failed_expectations=False)

# configure checkpoint and run it
context.add_or_update_checkpoint(
    name="my_quickstart_checkpoint",
    validator=validator,
)
checkpoint_result = context.run_checkpoint(checkpoint_name="my_quickstart_checkpoint")

# get results
stats = checkpoint_result.get_statistics()
print(next(iter(stats["validation_statistics"].values())))
# print(validator.validate().statistics) # does the same
