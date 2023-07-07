import great_expectations as ge
from great_expectations.checkpoint import Checkpoint
from pyspark.sql import SparkSession

# get context
context = ge.get_context()

# configure data source, data asset, batch, and expectation suite
dataframe_datasource = context.sources.add_or_update_spark(
    name="my_spark_in_memory_datasource"
)
spark = SparkSession.builder.getOrCreate()
df = spark.read.csv("yellow_taxi.csv", header=True)
dataframe_asset = dataframe_datasource.add_dataframe_asset(
    name="yellow_tripdata",
    dataframe=df,
)
batch_request = dataframe_asset.build_batch_request()
expectation_suite_name = "my_expectation_suite"
context.add_or_update_expectation_suite(expectation_suite_name=expectation_suite_name)

# configure validator
validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name=expectation_suite_name,
)
validator.expect_column_values_to_not_be_null("pickup_datetime")
validator.expect_column_values_to_be_between("passenger_count", 10, 20)
validator.save_expectation_suite(discard_failed_expectations=False)

# configure checkpoint
context.add_or_update_checkpoint(
    name="my_quickstart_checkpoint",
    validator=validator,
    expectation_suite_name=expectation_suite_name,
)
checkpoint_result = context.run_checkpoint(checkpoint_name="my_quickstart_checkpoint")

# Alternatively:
# checkpoint = Checkpoint(
    #     name="my_quickstart_checkpoint",
    #     run_name_template="%Y%m%d-%H%M%S-my-run-name-template",
    #     data_context=context,
    #     batch_request=batch_request,
    #     expectation_suite_name=expectation_suite_name,
    #     action_list=[
    #         {
    #             "name": "store_validation_result",
    #             "action": {"class_name": "StoreValidationResultAction"},
    #         },
    #         {"name": "update_data_docs", "action": {"class_name": "UpdateDataDocsAction"}},
    #     ],
    # )
    # context.add_or_update_checkpoint(checkpoint=checkpoint)


# get results
stats = checkpoint_result.get_statistics()
print(next(iter(stats["validation_statistics"].values())))
# print(validator.validate().statistics) # does the same
