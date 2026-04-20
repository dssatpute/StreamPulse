from pyspark import pipelines as dp


@dp.temporary_view(name="user_profile_dim_clean")
def user_profile_dim_clean():
    return spark.readStream.table("streampulse.bronze.user_profile_dim").select(
        "user_id",
        "username",
        "country",
        "email",
        "preferred_language",
        "subscription_plan",
        "subscription_status",
        "updated_at",
        "created_at",
        "profile_count",
    )


dp.create_streaming_table("streampulse.silver.user_profile_dim")

dp.create_auto_cdc_flow(
    target="streampulse.silver.user_profile_dim",
    source="user_profile_dim_clean",
    keys=["user_id"],
    sequence_by="updated_at",
    stored_as_scd_type=2,
    ignore_null_updates=False,
    apply_as_deletes=None,
    apply_as_truncates=None,
)


@dp.temporary_view(name="content_catalog_dim_clean")
def content_catalog_dim_clean():
    return spark.readStream.table("streampulse.bronze.content_catalog_dim").select(
        "title",
        "cast",
        "content_id",
        "content_type",
        "director",
        "duration_min",
        "episode_number",
        "genre",
        "language",
        "maturity_rating",
        "release_year",
        "season_number",
        "updated_at",
    )


dp.create_streaming_table("streampulse.silver.content_catalog_dim")

dp.create_auto_cdc_flow(
    target="streampulse.silver.content_catalog_dim",
    source="content_catalog_dim_clean",
    keys=["content_id"],
    sequence_by="updated_at",
    stored_as_scd_type=2,
    ignore_null_updates=False,
    apply_as_deletes=None,
    apply_as_truncates=None,
)
