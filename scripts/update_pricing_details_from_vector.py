from src.data_connectors.prices.valmer.time_series import ImportValmer
BUCKET_NAME = "Vector de precios"




ts_all_files = ImportValmer(
    bucket_name=BUCKET_NAME,
)

ts_all_files.update_pricing_details_from_last_vector(force_update=True)