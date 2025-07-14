"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel


def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """
    import os
    import zipfile
    import pandas as pd
    import glob

    os.makedirs("files/output", exist_ok=True)
    
    lista_datos = []
    
    archivos_zip = glob.glob("files/input/*.csv.zip")
    archivos_zip.sort()
    
    # Leer todos los archivos ZIP
    for archivo_zip in archivos_zip:
        with zipfile.ZipFile(archivo_zip, 'r') as zip_abierto:
            nombre_csv = zip_abierto.namelist()[0]
            with zip_abierto.open(nombre_csv) as archivo_csv:
                datos = pd.read_csv(archivo_csv)
                lista_datos.append(datos)
    
    datos_combinados = pd.concat(lista_datos, ignore_index=True)
    
    if 'Unnamed: 0' in datos_combinados.columns:
        datos_combinados = datos_combinados.drop('Unnamed: 0', axis=1)
    
    datos_combinados['client_id'] = datos_combinados.index
    
    # Client data
    columnas_cliente = ['client_id', 'age', 'job', 'marital', 'education', 'credit_default', 'mortgage']
    datos_cliente = datos_combinados[columnas_cliente].copy()
    
    datos_cliente['job'] = datos_cliente['job'].str.replace('.', '', regex=False)
    datos_cliente['job'] = datos_cliente['job'].str.replace('-', '_', regex=False)
    
    datos_cliente['education'] = datos_cliente['education'].str.replace('.', '_', regex=False)
    datos_cliente['education'] = datos_cliente['education'].replace('unknown', pd.NA)
    
    datos_cliente['credit_default'] = (datos_cliente['credit_default'] == 'yes').astype(int)
    datos_cliente['mortgage'] = (datos_cliente['mortgage'] == 'yes').astype(int)
    
    # Campaign data
    columnas_campana = ['client_id', 'number_contacts', 'contact_duration', 
                       'previous_campaign_contacts', 'previous_outcome', 
                       'campaign_outcome', 'day', 'month']
    datos_campana = datos_combinados[columnas_campana].copy()
    
    datos_campana['previous_outcome'] = (datos_campana['previous_outcome'] == 'success').astype(int)
    datos_campana['campaign_outcome'] = (datos_campana['campaign_outcome'] == 'yes').astype(int)
    
    # Crear fecha
    datos_campana['last_contact_date'] = pd.to_datetime(
        datos_campana['day'].astype(str) + '-' + 
        datos_campana['month'].astype(str) + '-2022', 
        format='%d-%b-%Y'
    ).dt.strftime('%Y-%m-%d')
    
    datos_campana = datos_campana.drop(['day', 'month'], axis=1)
    
    # Economics data
    columnas_economia = ['client_id', 'cons_price_idx', 'euribor_three_months']
    datos_economia = datos_combinados[columnas_economia].copy()
    
    # Guardar archivos
    datos_cliente.to_csv("files/output/client.csv", index=False)
    datos_campana.to_csv("files/output/campaign.csv", index=False)
    datos_economia.to_csv("files/output/economics.csv", index=False)
    
    return


if __name__ == "__main__":
    clean_campaign_data()