import pandas as pd
import dagster as dg
from plotnine import *
import io
import base64
import os


def correccion_nombre_islas(nombre):
    # Cambio de posición del artículo de los nombres que lo requieren ('Orotava, La' --> 'La Orotava')
    
    mapeo_sufijos= {
        ", El": "El",
        ", La": "La",
        ", Los": "Los",
        ", Las": "Las"
    }
    
    nombre_recortado = nombre.strip()
    for sufijo, prefijo in mapeo_sufijos.items():
        if nombre_recortado.endswith(sufijo):
            nombre_suelto = nombre_recortado[:-len(sufijo)].strip()
            return f"{prefijo} {nombre_suelto}"
    return nombre_recortado


@dg.asset
def cargar_distrib_renta(context: dg.AssetExecutionContext):

    df = pd.read_csv('distribucion-renta-canarias.csv')

    context.log.info(f"Carga del contenido de distribucion-renta-canarias.csv")
    
    return df


@dg.asset
def cargar_codigo_islas(context: dg.AssetExecutionContext):

    codislas_df = pd.read_csv('codislas.csv', sep=';', encoding='latin1')

    context.log.info("Carga del contenido de codislas.csv. Se tuvo que poner encoding=latin1 porque había caracteres que no eran reconocidos en UTF-8.")
    
    return codislas_df


@dg.asset
def aplicacion_formateo(context: dg.AssetExecutionContext, cargar_codigo_islas: pd.DataFrame):
    
    codislas_formateo = cargar_codigo_islas.copy()
    codislas_formateo['NOMBRE'] = codislas_formateo['NOMBRE'].apply(correccion_nombre_islas)
    codislas_formateo['ISLA'] = codislas_formateo['ISLA'].apply(correccion_nombre_islas)

    context.log.info("Corrección de los nombres de las islas y municipios ('Orotava, La' --> 'La Orotava')")

    return codislas_formateo


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
def merge_renta_codislas(context: dg.AssetExecutionContext, cargar_distrib_renta: pd.DataFrame, aplicacion_formateo: pd.DataFrame):

    df = pd.merge(
        cargar_distrib_renta,
        aplicacion_formateo[['NOMBRE', 'ISLA']],
        left_on='TERRITORIO#es',
        right_on='NOMBRE',
        how='left'
    )

    df['ISLA'] = df['ISLA'].fillna("Sin clasificación")

    context.log.info("Unión entre el set de datos de renta con los de los códigos de las islas")

    return df.drop(columns=['NOMBRE'], errors='ignore')


@dg.asset
def preparacion_data_plot(context: dg.AssetExecutionContext, merge_renta_codislas: pd.DataFrame):
    # Se agrupan los valores por territorio, tipo de medida e isla y se calculan los valores medios
    # Con el comando unstack, lo que hace es desglosar los diferentes tipos de medidas en diferentes columnas con los mismos nombres
    medias_df = merge_renta_codislas.groupby(['TERRITORIO_CODE', 'MEDIDAS#es', 'ISLA'])['OBS_VALUE'].mean().unstack(level='MEDIDAS#es')
    
    unique_medidas = medias_df.columns.tolist()

    # Con el comando unstack anterior, TERRITORIO_CODE e ISLA pasaron a ser índices. Para reestablecer un índice numérico
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
    
    # Mapa de colores para los diferentes puntos
    custom_color_map = {
        'Sin clasificación': 'red', 
        'Fuerteventura': 'dodgerblue',
        'Gran Canaria': 'limegreen',
        'La Gomera': 'darkred',
        'La Palma': 'coral',
        'Lanzarote': 'khaki',
        'El Hierro': 'olive',
        'Tenerife': 'mediumvioletred'
    }

    # Mapa de formas para los diferentes puntos
    custom_shapes_map = {
        'Sin clasificación': 'x', 
        'Fuerteventura': 'o',
        'Gran Canaria': 's',
        'La Gomera': '^',
        'La Palma': 'd',
        'Lanzarote': 'v',
        'El Hierro': '*',
        'Tenerife': 'p'
    }

    metadata_dict = {}

    for idx, (x_col, y_col) in enumerate(medidas):

        p = (
            ggplot(
                plot_data, 
                aes(x=x_col, y=y_col)
            )
            + geom_point(
                aes(color='ISLA', shape='ISLA'), 
                size=3, 
                alpha=0.7
            )
            + geom_smooth(
                method='lm', 
                color='black', 
                alpha=0.2, 
                linetype='dashed', 
                show_legend=False
            )
            + labs(
                title=f'{x_col} vs {y_col}',
                x=x_col,
                y=y_col,
                color='Isla',
                shape='Isla'
            )
            + scale_color_manual(values=custom_color_map)
            + scale_shape_manual(values=custom_shapes_map)
            + theme_light()
            + theme(
                plot_title=element_text(size=14, ha='center'),
                legend_position='bottom',
                legend_box='horizontal',
                legend_direction='horizontal'
            )
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