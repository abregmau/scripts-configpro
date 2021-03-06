### Librerías
import pandas as pd
import numpy as np
from time import time

### Funciones

def leer_conf(agru_file):
    
    # Leer pestaña ALAR
    file = pd.read_excel(agru_file, sheet_name='ALAR')
    df_alar = pd.DataFrame(file)

    # Convertir números en enteros
    df_alar['Micro'] = np.array(df_alar['Micro'], dtype=int)
    df_alar['Local Point'] = np.array(df_alar['Local Point'], dtype=int)
    df_alar['Inicio'] = np.array(df_alar['Inicio'], dtype=int)
    df_alar['Nro. Alarmas'] = np.array(df_alar['Nro. Alarmas'], dtype=int)

    #print (df_alar.dtypes)

    # Leer pestaña DATA
    file = pd.read_excel(agru_file, sheet_name='DATA')
    
    df_data = pd.DataFrame(file)

    # Convertir en números enteros
    df_data['Micro'] = np.array(df_data['Micro'], dtype=int)
    df_data['Índice'] = np.array(df_data['Índice'], dtype=int)
    df_data['LP_Agru'] = np.array(df_data['LP_Agru'], dtype=int)
    
    #print (df_data.dtypes)


    return df_alar, df_data

def procesa_agru(df_alar, df_data, m): # Función Procesar agrupamientos
    
    df_alar_exp = pd.DataFrame( columns= ['Operación', 'Inicio', 'Nro. Alarmas', 'Fechado', 'aux1','aux2'])
    df_data_exp = pd.DataFrame( columns= ['TXT', 'Alarma','Fechado','Status', 'aux1'])

    df_alar.sort_values(['Micro','Local Point'], inplace=True) # Ordenar tabla ALAR

    df_alar = df_alar[df_alar['Micro'] == m] # Filtrar tabla por micro
    df_alar = df_alar.reset_index(drop=True)

    df_data = df_data[df_data['Micro'] == m] # Filtrar tabla por micro
    df_data = df_data.reset_index(drop=True)

    df_descripcion = df_alar['Descripción'] # Generar dataframe para exporta DIDESC

    # Generar indices y constates para iteración

    row_alar = range(0, df_alar.shape[0])
    row_data = range(0, df_data.shape[0])
    len_row_alar = len(row_alar)
    len_row_data = len(row_data)

    # Iteración para procesamiento de agrupadas
    
    # i = índice de iteración sobre dataframe ALAR
    # j = índice de iteración sobre dataframe DATA
    # h = contador de entradas agrupada
    # k = índice de ordenamiento DATA sobre dataframe a exportar

    k=0

    for i in row_alar: # Iteración sobre ALAR
        h=0
        print('Procesando agrupada nº', i+1, 'de', len_row_alar,'Micro', m)
        df_data1 = df_data[df_data['LP_Agru'] == i+1] # Filtrar tabla por agrupada
        df_data1 = df_data1.reset_index(drop=True)

        row_data = row_data = range(0, df_data1.shape[0])
        for j in row_data: # Iteración sobre DATA

            if df_data1.loc[j,'LP_Agru'] == df_alar.loc[i,'Local Point']: # Buscar las entradas en DATA que
                # pertenecen al punto i de AGRU

                # Generar el campo TXT: (XXXXXX) DESCRIPTION
                a = str(df_data1.loc[j,'Syspoint'])
                b = str(df_data1.loc[j,'Descripción'])

                while len(a) < 6:
                    a = "0" + a
                a = '(' + a + ') ' + b
                df_data_exp.loc[k,'TXT'] = a # Texto
                df_data_exp.loc[k,'Alarma'] = df_data1.loc[j,'Alarma']
                df_data_exp.loc[k,'Fechado'] = df_data1.loc[j,'Fechado']
                df_data_exp.loc[k,'Status'] = df_data1.loc[j,'Status']
                df_data_exp.loc[k,'aux1'] = '0'

                # Actualizar índices
                k +=1
                h +=1

        LP = df_alar.loc[i, 'Local Point']

        # Cargar datos sobre dataframe export ALAR

        if h != 0: # Completar configuración agrupada solo si existen entradas en DATA
            df_alar_exp.loc[LP-1, 'Operación'] = df_alar.loc[i, 'Operación']
            df_alar_exp.loc[LP-1, 'Inicio'] = k +1 - h
            df_alar_exp.loc[LP-1, 'Nro. Alarmas'] = h
            df_alar_exp.loc[LP-1, 'Fechado'] = df_alar.loc[i, 'Fechado']
            df_alar_exp.loc[LP-1,'aux1'] = '0'
            df_alar_exp.loc[LP-1,'aux2'] = '0'

    return df_alar_exp, df_data_exp, df_descripcion
