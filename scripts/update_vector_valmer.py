from src.data_connectors.prices.valmer.time_series import ImportValmer
BUCKET_NAME = "Vector de precios"

for i in range(360//5):
    ts_all_files = ImportValmer(
        bucket_name=BUCKET_NAME,
    )

    ts_all_files.run(
        debug_mode=True,
        force_update=True,
    )