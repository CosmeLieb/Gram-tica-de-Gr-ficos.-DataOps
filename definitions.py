from dagster import Definitions, load_assets_from_modules, load_asset_checks_from_modules, define_asset_job, AssetSelection
import lab_renta
from sensor import my_directory_sensor

assets_prompt=load_assets_from_modules([lab_renta])
# 2. Definimos el Job (la unión de los assets)
pipeline_ia_job = define_asset_job(
    name="job_visualizacion_ia",
    selection=AssetSelection.all()
)

defs = Definitions(
    assets=load_assets_from_modules([lab_renta]),
    asset_checks=load_asset_checks_from_modules([lab_renta]),
    jobs=[pipeline_ia_job],
    sensors=[my_directory_sensor]
)