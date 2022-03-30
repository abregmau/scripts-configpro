### Librerías
import pandas as pd
import numpy as np
from time import time
import csv

### Funciones

def lectura(bd):

    print('Leyendo Base de Datos', bd)
    file = pd.read_csv(bd, sep=',', index_col=False)
    df_bd = pd.DataFrame(file)
    #print (reg.dtypes)

    df_bd = df_bd[df_bd.Description.notnull()] # Limpiar vacías en Description
    df_bd = df_bd.drop(['Tag 1 Description', 'Tag 2 Description', 'Tag 3 Description'], axis=1)
    df_bd = df_bd.reset_index(drop=True)
    
    print('Base de datos leída con éxito')
    return df_bd

def escritura_xlsx(bd, df, disp):

    with pd.ExcelWriter(bd) as writer: # pylint: disable=abstract-class-instantiated
        df.to_excel(writer, sheet_name= disp, index=False)
    
    print('Escritura ' + bd + ' completa')

def lectura_cot(cot, b021):

    print('Leyendo datos COT', cot)

    # Leer archivo COT
    file = pd.read_excel(cot, sheet_name='DI')
    df_cot = pd.DataFrame(file)
    df_cot = df_cot[['Name', 'FEP Key', 'Point Address']]

    # Leer B021MT01
    lista_b021_mt01 = []
    i=1
    csvarchivo = open(b021)  # Abrir archivo csv
    reg = csv.reader(csvarchivo)  # Leer todos los registros 
    
    for row in reg:
        entrada, invert, cos, soe, eventclass = row # Desempaquetar campos registro
        syspoint = entrada[1:7] # Extrae systempoint
        if syspoint != '______': # Convierte en valores systempoints
            syspoint = int(syspoint)
        txt = entrada[9:] # Extrae TXT
        lista_b021_mt01.append([ i, syspoint, txt, invert, cos, soe, eventclass])
        i=i+1

    enc_b021_mt01 = ['DNP', 'Syspoint', 'Description', 'Invert Status', 'COS', 'SOE', 'Event Class']
    df_b021_mt01 = pd.DataFrame(data=lista_b021_mt01, columns=enc_b021_mt01)

    # Combinar datos COT con B021
    df_cot_p = pd.merge(left= df_b021_mt01, right= df_cot, how='outer', left_on= 'DNP', right_on= 'Point Address')
    df_cot_p = df_cot_p.drop('Point Address', axis= 1)
    df_cot_p = df_cot_p.drop('COS', axis= 1)
    df_cot_p = df_cot_p.drop('SOE', axis= 1)
    df_cot_p = df_cot_p.drop('Event Class', axis= 1)

    return df_cot_p
    

def procesa(df_bd, df_data):

    print('Procesando archivos, generando base de datos unificada...')

    # Variables Auxiliares
    df_bd_p = df_bd[df_bd['Point Type'] == 'DI']
    df_bd_aux1 = df_bd[df_bd['Point Type'] != 'DI']
    df_bd_aux2 = pd.DataFrame()
    df_bd_aux2[['SP_Agru', 'LP_Agru', 'TXT_Agru']] = df_bd_p[['System Point', 'Local Point', 'Description']]
    df_data_aux = df_data[['Syspoint', 'Alarma', 'Operación', 'TXT_Agru', 'LP_Agru', 'Micro']]

    # Combinar bd con data
    df_bd_p = pd.merge(left= df_bd_p, right= df_data_aux, how='left', left_on= 'System Point', right_on= 'Syspoint')
    #Combinar SP agrupamientos en BD
    df_bd_p = pd.merge(left= df_bd_p, right= df_bd_aux2, how='left', left_on= ['TXT_Agru', 'LP_Agru'], right_on= ['TXT_Agru', 'LP_Agru'])

    # Regenerar bd
    df_bd_p = pd.concat([ df_bd_p, df_bd_aux1], sort= False)
    df_bd_p = df_bd_p.sort_values(['Point Type', 'System Point'])
    df_bd_p = df_bd_p.reset_index(drop= True)
    df_bd_p = df_bd_p.drop('Syspoint', axis= 1)

    print('Base de datos generada')
    return df_bd_p