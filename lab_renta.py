import pandas as pd
from dagster import asset, AssetExecutionContext, asset_check, AssetCheckResult, MetadataValue, Output
from plotnine import *
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

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
            return f"{prefijo} {nombre_suelto}".upper()
    return nombre_recortado.upper()


'''
=========================================================
CARGA DE DATOS Y CHECKS distribucion-renta-canarias.csv
=========================================================
'''
@asset
def cargar_distrib_renta(context: AssetExecutionContext):

    path = os.path.join(BASE_DIR, 'distribucion-renta-canarias.csv')
    df = pd.read_csv(path)

    # Debido a problemas detectados con los checks, se va a añadir la siguiente parte para que en el dataframe se distingan las provincias y los municipios que comparten nombre
    df.loc[df['TERRITORIO_CODE'].str.startswith('ES'), 'TERRITORIO#es'] = df.loc[df['TERRITORIO_CODE'].str.startswith('ES'), 'TERRITORIO#es'] + ' (isla/provincia)'

    context.log.info(f"Carga del contenido de distribucion-renta-canarias.csv")
    
    return df


@asset_check(asset=cargar_distrib_renta)
def check_estandarizacion_territorio_es(islas_raw):
    originales = islas_raw['TERRITORIO#es'].nunique()
    normalizadas = islas_raw['TERRITORIO#es'].str.capitalize().nunique()
    
    passed = originales == normalizadas
    
    return AssetCheckResult(
        passed = passed,
        metadata = {
            "categorias_detectadas": MetadataValue.int(originales),
            "categorias_esperadas": MetadataValue.int(normalizadas),
            "principio_gestalt": "Similitud (Evitar fragmentación visual)",
            "descripcion": "Si hay nombres inconsistentes, ggplot creará leyendas duplicadas."
        }
    )


@asset_check(asset=cargar_distrib_renta)
def check_estandarizacion_medidas_es(df):
    originales = df['MEDIDAS#es'].nunique()
    normalizadas = df['MEDIDAS#es'].str.capitalize().nunique()
    
    passed = originales == normalizadas
    
    return AssetCheckResult(
        passed = passed,
        metadata = {
            "categorias_detectadas": MetadataValue.int(originales),
            "categorias_esperadas": MetadataValue.int(normalizadas),
            "principio_gestalt": "Similitud (Evitar fragmentación visual)",
            "descripcion": "Si hay nombres inconsistentes, ggplot creará leyendas duplicadas."
        }
    )


@asset_check(asset=cargar_distrib_renta)
def check_num_entadas_dif_TERRITORIO_ES_CODE(df):
    es = df['TERRITORIO#es'].nunique()
    code = df['TERRITORIO_CODE'].nunique()

    passed = es == code

    return AssetCheckResult(
        passed = passed,
        metadata = {
            "entradas_detectadas_TERRITORIO#es": MetadataValue.int(es),
            "entradas_detectadas_TERRITORIO_CODE": MetadataValue.int(code),
            "principio_gestalt": "Similitud / Correspondencia",
            "descripcion": "Al comprobar el número de variables diferentes en las dos columnas, se evita asignar diferentes representaciones a valores que deberían ser asignados la misma etiqueta."
        }
    )


@asset_check(asset=cargar_distrib_renta)
def verificar_no_nulos_renta(cargar_distrib_renta):
    # Se genera un array booleano indicando si hay valores nulos o no en las dos columnas seleccionadas
    # Luego se suman por separado para tener el valor de nulos en cada columna y estos se suman para saber el total
    valores_nulos = cargar_distrib_renta[['OBS_VALUE', 'TERRITORIO_CODE']].isnull().sum().sum()

    passed = valores_nulos == 0

    return AssetCheckResult(
        passed = bool(passed),
        metadata = {
            "valores_nulos": MetadataValue.int(int(valores_nulos)),
            "principio_gestalt": "Figura y Fondo",
            "descripcion": "Los huecos inesperados rompen la forma de la visualización."
        }
    )


@asset_check(asset=cargar_distrib_renta)
def check_rango_valores_renta(df):

    valor_min = df["OBS_VALUE"].min()
    valor_max = df["OBS_VALUE"].max()

    passed = (valor_min >= 0) and (valor_max <= 1000)

    return AssetCheckResult(
        passed= bool(passed),
        metadata={
            "valor_minimo": MetadataValue.float(float(valor_min)),
            "valor_maximo": MetadataValue.float(float(valor_max)),
            "principio_gestalt": "Continuidad / Buena forma",
            "descripcion": "Valores fuera de rango indican errores de datos."
        }
    )


@asset_check(asset=cargar_distrib_renta)
def check_duplicados_renta(df):

    duplicados = df.duplicated().sum()
    
    passed = duplicados == 0
    
    return AssetCheckResult(
        passed = bool(passed),
        metadata={
            "filas_duplicadas": MetadataValue.int(int(duplicados)),
            "principio_gestalt": "Similitud",
            "descripcion": "Cuando ocurre el duplicado de entradas, se refuerza de forma incorrecta la categoría afectada."
        }
    )


'''
======================================
CARGA DE DATOS codislas.csv
======================================
'''
@asset
def cargar_codigo_islas(context: AssetExecutionContext):

    path = os.path.join(BASE_DIR, 'codislas.csv')
    codislas_df = pd.read_csv(path, sep=';', encoding='latin1')

    context.log.info("Carga del contenido de codislas.csv. Se tuvo que poner encoding=latin1 porque había caracteres que no eran reconocidos en UTF-8.")
    
    return codislas_df


'''
==============================================
CORRECCIÓN NOMBRES DE ENTRADAS DE codislas.csv
==============================================
'''
@asset
def aplicacion_formateo(context: AssetExecutionContext, cargar_codigo_islas: pd.DataFrame):
    
    codislas_formateo = cargar_codigo_islas.copy()
    codislas_formateo['NOMBRE'] = codislas_formateo['NOMBRE'].apply(correccion_nombre_islas)
    codislas_formateo['ISLA'] = codislas_formateo['ISLA'].apply(correccion_nombre_islas)

    context.log.info("Corrección de los nombres de las islas y municipios ('Orotava, La' --> 'La Orotava')")

    return codislas_formateo


'''
======================
CHECKS DE codislas.csv
======================
'''
@asset_check(asset=aplicacion_formateo)
def check_estandarizacion_codislas_nombres(df):
    originales = df['NOMBRE'].nunique()
    normalizadas = df['NOMBRE'].str.capitalize().nunique()
    
    passed = originales == normalizadas
    
    return AssetCheckResult(
        passed = passed,
        metadata = {
            "categorias_detectadas": MetadataValue.int(originales),
            "categorias_esperadas": MetadataValue.int(normalizadas),
            "principio_gestalt": "Similitud (Evitar fragmentación visual)",
            "descripcion": "Si hay nombres inconsistentes, ggplot creará leyendas duplicadas."
        }
    )


@asset_check(asset=aplicacion_formateo)
def check_estandarizacion_codislas_islas(df):
    originales = df['ISLA'].nunique()
    normalizadas = df['ISLA'].str.capitalize().nunique()
    
    passed = originales == normalizadas
    
    return AssetCheckResult(
        passed=passed,
        metadata={
            "categorias_detectadas": MetadataValue.int(originales),
            "categorias_esperadas": MetadataValue.int(normalizadas),
            "principio_gestalt": "Similitud (Evitar fragmentación visual)",
            "descripcion": "Si hay nombres inconsistentes, ggplot creará leyendas duplicadas."
        }
    )


@asset_check(asset=aplicacion_formateo)
def verificar_no_nulos_codislas(aplicacion_formateo):
    valores_nulos_isla = aplicacion_formateo['ISLA'].isnull().sum()
    valores_nulos_nombre = aplicacion_formateo['NOMBRE'].isnull().sum()

    if (valores_nulos_isla == 0) and (valores_nulos_nombre == 0):
        passed = True

    else:
        passed = False

    return AssetCheckResult(
        passed = bool(passed),
        metadata = {
            "valores_nulos_isla": MetadataValue.int(int(valores_nulos_isla)),
            "valores_nulos_nombre": MetadataValue.int(int(valores_nulos_nombre)), 
            "principio_gestalt": "Figura y Fondo",
            "descripcion": "Los huecos inesperados rompen la forma de la visualización."
            }
    )


@asset_check(asset=aplicacion_formateo)
def check_duplicados_codislas(df):

    duplicados = df.duplicated().sum()

    passed = duplicados == 0

    return AssetCheckResult(
        passed = bool(passed),
        metadata={
            "filas_duplicadas": MetadataValue.int(int(duplicados)),
            "principio_gestalt": "Similitud",
            "descripcion": "Cuando ocurre el duplicado de entradas, se refuerza de forma incorrecta la categoría afectada."
        }
    )


'''
==========================================
UNIÓN ENTRE LOS DOS SETS DE DATOS Y CHECKS
==========================================
'''
@asset
def merge_renta_codislas(context: AssetExecutionContext, cargar_distrib_renta: pd.DataFrame, aplicacion_formateo: pd.DataFrame):

    cargar_distrib_renta['TERRITORIO#es'] = cargar_distrib_renta['TERRITORIO#es'].apply(lambda x: x.upper() if isinstance(x, str) else x)

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


@asset_check(asset=merge_renta_codislas)
def check_numero_municipios(df):
    
    num_municipios = df[df["ISLA"] != "Sin clasificación"]["TERRITORIO_CODE"].nunique()

    # Valor esperado de municipios de Canarias
    esperado = 88

    passed = num_municipios == esperado

    return AssetCheckResult(
        passed = bool(passed),
        metadata = {
            "municipios_detectados": MetadataValue.int(int(num_municipios)),
            "municipios_esperados": MetadataValue.int(int(esperado)),
            "principio_gestalt": "Cierre",
            "descripcion": "Se excluyen Canarias, provincias e islas. Solo se cuentan los municipios."
        }
    )


@asset_check(asset=merge_renta_codislas)
def check_merge_sin_filas_perdidas(df):

    filas = len(df)

    passed = filas > 0

    return AssetCheckResult(
        passed = bool(passed),
        metadata={
            "filas_totales": MetadataValue.int(int(filas)),
            "principio_gestalt": "Continuidad",
            "descripcion": "En el caso de perderse filas, se rompe la estructura de los datos."
        }
    )


'''
===================================
PREPARACIÓN CONTENIDO PARA GRAFICAR
===================================
'''
@asset
def preparacion_data_plot(context: AssetExecutionContext, merge_renta_codislas: pd.DataFrame):
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


@asset_check(asset=preparacion_data_plot)
def check_cardinalidad(merge):
    plot_data = merge['data']
    num_islas = plot_data['ISLA'].nunique()
    passed = num_islas <= 9

    return AssetCheckResult(
        passed = bool(passed),
        metadata = {
            "islas_detectadas": MetadataValue.int(int(num_islas)),
            "principio_gestalt": "Carga Cognitiva / Similitud",
            "descripcion": "Más de 9 colores son imposibles de distinguir."
        }
    )

'''
=============================
VISUALIZACIÓN DE LAS GRÁFICAS
=============================
'''

# Mapa de colores para los diferentes puntos
CUSTOM_COLOR_MAP = {
    'Sin Clasificación': 'red', 
    'Fuerteventura': 'dodgerblue',
    'Gran Canaria': 'limegreen',
    'La Gomera': 'darkred',
    'La Palma': 'coral',
    'Lanzarote': 'khaki',
    'El Hierro': 'olive',
    'Tenerife': 'mediumvioletred'
}

# Mapa de formas para los diferentes puntos
CUSTOM_SHAPES_MAP = {
    'Sin Clasificación': 'x', 
    'Fuerteventura': 'o',
    'Gran Canaria': 's',
    'La Gomera': '^',
    'La Palma': 'd',
    'Lanzarote': 'v',
    'El Hierro': '*',
    'Tenerife': 'p'
}


@asset
def visualizacion(context: AssetExecutionContext, preparacion_data_plot):
    plot_data = preparacion_data_plot["data"]
    medidas = preparacion_data_plot["medidas"]
    
    plot_data['ISLA'] = plot_data['ISLA'].str.title()
    
    # Dirección donde se van a guardar las figuras
    ruta_base = os.path.join(BASE_DIR, "Gráficos")
    
    if not os.path.exists(ruta_base):
        os.makedirs(ruta_base)

    context.log.info(f"Guardando gráficos en: {ruta_base}")

    # Mapa de colores para los diferentes puntos
    custom_color_map = CUSTOM_COLOR_MAP

    # Mapa de formas para los diferentes puntos
    custom_shapes_map = CUSTOM_SHAPES_MAP

    metadata_dict = {}

    for idx, (x_col, y_col) in enumerate(medidas):

        context.log.info(f"Generando gráfico {idx+1}/{len(medidas)}")
        
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

        context.add_output_metadata({
            "archivo": MetadataValue.path(os.path.join(ruta_base, filename))
            })
        
        metadata_dict[filename] = MetadataValue.path(
            os.path.join(ruta_base, filename)
            )
    
    return Output(
        value = f"Se han generado {len(medidas)} gráficos en {ruta_base}",
        metadata=metadata_dict
    )


@asset_check(asset=preparacion_data_plot)
def check_custom_color_map(df):

    plot_data = df["data"]

    passed = plot_data["ISLA"].nunique() == len(CUSTOM_COLOR_MAP.keys())

    return AssetCheckResult(
        passed=passed,
        metadata={
            "num_islas_datos": MetadataValue.int(int(plot_data["ISLA"].nunique())),
            "num_islas_color_map": MetadataValue.int(int(len(CUSTOM_COLOR_MAP.keys()))),
            "principio_gestalt": "Similitud / Consistencia",
            "descripcion": "Si una categoría no se le asigna un color, se rompe la asociación visual."
        }
    )


@asset_check(asset=preparacion_data_plot)
def check_custom_shapes_map(df):

    plot_data = df["data"]

    passed = plot_data["ISLA"].nunique() == len(CUSTOM_SHAPES_MAP.keys())

    return AssetCheckResult(
        passed=passed,
        metadata={
            "num_islas_datos": MetadataValue.int(int(plot_data["ISLA"].nunique())),
            "num_islas_shapes_map": MetadataValue.int(int(len(CUSTOM_SHAPES_MAP.keys()))),
            "principio_gestalt": "Similitud / Consistencia",
            "descripcion": "Si una categoría no se le asigna una forma, se rompe la asociación visual."
        }
    )