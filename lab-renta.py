import pandas as pd
import dagster as dg
from plotnine import *
import io
import base64
import os

@dg.asset
def cargar_distrib_renta(context: dg.AssetExecutionContext):

    df = pd.read_csv('distribucion-renta-canarias.csv')

    context.log.info(f"Carga del contenido de distribucion-renta-canarias.csv")
    
    return df


@dg.asset_check(asset=cargar_distrib_renta)
def verificar_numero_municipios(cargar_distrib_renta):

    num_territorios_distintos = cargar_distrib_renta['TERRITORIO_CODE'].nunique()

    return dg.AssetCheckResult(
        passed=bool(num_territorios_distintos > 88),
        metadata={'num_territorios_distintos': int(num_territorios_distintos),
                  'nota': "Se comprueba que haya como mínimo los 88 municipios (hay más porque se incluyen los nombres de las islas y de la provincia en el set de datos)"
        }
    )


@dg.asset_check(asset=cargar_distrib_renta)
def verificar_no_nulos(cargar_distrib_renta):
    # Se genera un array booleano indicando si hay valores nulos o no en las dos columnas seleccionadas
    # Luego se suman por separado para tener el valor de nulos en cada columna y estos se suman para saber el total
    valores_nulos = cargar_distrib_renta[['OBS_VALUE', 'TERRITORIO_CODE']].isnull().sum().sum()

    return dg.AssetCheckResult(
        passed=bool(valores_nulos == 0),
        metadata={"valores_nulos": int(valores_nulos)}
    )


@dg.asset
def preparacion_data_plot(context: dg.AssetExecutionContext, cargar_distrib_renta: pd.DataFrame):
    # Se agrupan los valores por territorio y tipo de medida y se calculan los valores medios
    # Con el comando unstack, lo que hace es desglosar los diferentes tipos de medidas en diferentes columnas con los mismos nombres
    medias_df = cargar_distrib_renta.groupby(['TERRITORIO_CODE', 'MEDIDAS#es'])['OBS_VALUE'].mean().unstack(level='MEDIDAS#es')
    
    unique_medidas = medias_df.columns.tolist()

    # Con el comando unstack anterior, TERRITORIO_CODE pasó a ser índices. Para reestablecer un índice numérico
    # con el que trabajar más fácilmente, se tiene que usar reset_index().
    plot_data = medias_df.reset_index()
    
    # Generación de una lista que contiene tuplas de las combinaciones de las diferentes medidas
    columnas_con_datos = []
    for i in range(len(unique_medidas)):
        for j in range(i + 1, len(unique_medidas)):
            columnas_con_datos.append((unique_medidas[i], unique_medidas[j]))

    context.log.info(f"Columnas detectadas con datos reales: {columnas_con_datos}")
    
    return {"medidas": columnas_con_datos, "data": plot_data}


@dg.asset
def visualizacion(preparacion_data_plot):
    plot_data = preparacion_data_plot["data"]
    medidas = preparacion_data_plot["medidas"]
    
    # Dirección donde se van a guardar las figuras
    ruta_base = "/mnt/c/Users/CosmeLD/Desktop/MASTER ULL/3r Bimestre/VISUALIZACIÓN/Práctica_2/Gráficos"
    if not os.path.exists(ruta_base):
        os.makedirs(ruta_base)

    metadata_dict = {}

    for idx, (x_col, y_col) in enumerate(medidas):

        p = (
            ggplot(
                plot_data, 
                aes(x=x_col, y=y_col)
            )
            + geom_point( 
                size=3, 
                alpha=0.7,
                color='blue'
            )
            + geom_smooth(
                method='lm', 
                color='black', 
                alpha=0.2, 
                linetype='dashed'
            )
            + labs(
                title=f'{x_col} vs {y_col}',
                x=x_col,
                y=y_col
            )
            + theme_light()
            + theme(plot_title=element_text(size=14, ha='center'))
        )


        filename = f"plot_{x_col}_vs_{y_col}.png"
        p.save(os.path.join(ruta_base, filename), verbose=False)

        # Preparar para Dagster UI
        buf = io.BytesIO()
        p.save(buf, format='png', verbose=False)
        img_str = base64.b64encode(buf.getvalue()).decode("utf-8")
        
        # Usamos el nombre del archivo como llave para que no se sobrescriban
        metadata_dict[filename] = dg.MetadataValue.md(
            f"![{filename}](data:image/png;base64,{img_str})"
        )

    return f"Se han generado {len(medidas)} gráficos en {ruta_base}"