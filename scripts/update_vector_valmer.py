from src.data_connectors.prices.valmer.time_series import ImportValmer
BUCKET_NAME = "Vector de precios"



first_time_update_loop=False
ts_all_files = ImportValmer(
    bucket_name=BUCKET_NAME,
)
try:
    us = ts_all_files.get_update_statistics()
except AttributeError:
# first time update
    first_time_update_loop = True
if first_time_update_loop ==True:
    for i in range(360//5):
        ts_all_files = ImportValmer(
            bucket_name=BUCKET_NAME,
        )
        ts_all_files.run(
            debug_mode=True,
            force_update=True,
        )
else:
    ts_all_files = ImportValmer(
        bucket_name=BUCKET_NAME,
    )
    ts_all_files.run(
        debug_mode=True,
        force_update=True,
    )