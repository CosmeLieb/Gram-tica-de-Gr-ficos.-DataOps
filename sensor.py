import os
from dagster import sensor, RunRequest, AssetSelection, define_asset_job

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# 1. Definimos el JOB (el pipeline completo)
# Esto le dice a Dagster: "Quiero ejecutar desde el origen hasta el final"
pipeline_islas_job = define_asset_job(
    name="pipeline_islas_job",
    selection=AssetSelection.all()
)

# 2. Definimos el SENSOR
@sensor(job=pipeline_islas_job)
def my_directory_sensor(context):
    archivos_vigilados = [
        os.path.join(BASE_DIR, "distribucion-renta-canarias.csv"),
        os.path.join(BASE_DIR, "codislas.csv")
    ]
    
    # Verificación de seguridad: ¿existe el fichero?
    for ruta in archivos_vigilados:
        if not os.path.exists(ruta):
            context.log.warning(f"Fichero {ruta} no encontrado.")
            return

    # Lógica del cursor (tu código estaba perfecto aquí)
    last_mtime = context.cursor or "0"
    curr_mtime = "_".join(str(os.path.getmtime(ruta)) for ruta in archivos_vigilados)
    
    if curr_mtime != last_mtime:
        context.log.info("Detectado cambio en los CSV. Lanzando pipeline...")
        context.update_cursor(curr_mtime)
        yield RunRequest(run_key=curr_mtime)