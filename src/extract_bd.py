### Librerías
import pandas as pd
from pandas import ExcelWriter
import numpy as np
from time import time

### Funciones

def lectura(bd):

    print('Leyendo Base de Datos', bd)
    file = pd.read_csv(bd, sep=',', index_col=False)
    reg = pd.DataFrame(file)
    #print (reg.dtypes)

    reg = reg[reg.Description.notnull()] # Limpiar vacías en Description
    reg = reg.drop(['Tag 1 Description', 'Tag 2 Description', 'Tag 3 Description'], axis=1)
    reg = reg.reset_index(drop=True)
    
    print('Base de datos leída con éxito')
    return reg

def escritura_xlsx(bd, df, disp):

    with ExcelWriter(bd) as writer: # pylint: disable=abstract-class-instantiated
        df.to_excel(writer, sheet_name= disp, index=False)
    
    print('Escritura ' + bd + ' completa')

def procesa(df_bd, df_data):

    print('Procesando archivos, generando base de datos unificada...')

    df_bd_f = df_bd[df_bd['Point Type'] == 'DI'] # Filtrado por DI
    df_bd_p = df_bd.copy()

    df_bd_p['Alarma'] = '' # Crear columna para escribir
    df_bd_p['Operación'] = '' # Crear columna para escribir
    df_bd_p['TXT_Agru'] = '' # Crear columna para escribir
    df_bd_p['SP_Agru'] = '' # Crear columna para escribir

    tot = len(df_bd_f)
    h=0
    q=0

    for i in df_bd_f.index.tolist(): # Iterar sobre base de datos
        if q < 100:
            q +=1
            h +=1
        else:
            print('Porcentaje procesado: {0:.3}%'.format(h/tot*100))
            q =0
            h +=1
        
        df_data_i = df_data[df_data['Syspoint'] == df_bd_f.loc[i,'System Point']] # Filtrar data por Syspoint
        df_data_i = df_data_i.reset_index(drop=True)

        len_data_i = len(df_data_i)

        if len_data_i >= 1:
            df_bd_p.loc[i,[ 'Alarma', 'Operación', 'TXT_Agru']] = df_data_i.loc[0,['Alarma', 'Operación', 'TXT_Agru']]
            #df_bd_p.loc[i, 'Alarma'] = df_data_i.loc[0, 'Alarma']
            #df_bd_p.loc[i, 'Operación'] = df_data_i.loc[0, 'Operación']
            #df_bd_p.loc[i, 'TXT_Agru'] = df_data_i.loc[0, 'TXT_Agru']

            aux = df_bd_p[df_bd_p['Description'] == df_data_i.loc[0,'TXT_Agru']] # Buscar agrupada
            aux = aux.reset_index(drop=True)
            df_bd_p.loc[i,'SP_Agru'] = aux.loc[0,'System Point']

        if len_data_i > 1:
            j=1
            while j < len_data_i:
                df_bd_p = df_bd_p.append(df_bd_p.loc[i] , ignore_index=True) # Duplicar
                
                df_bd_p.loc[len(df_bd_p)-1,['Alarma', 'Operación', 'TXT_Agru']] = df_data_i.loc[j, ['Alarma', 'Operación', 'TXT_Agru']]

                df_bd_p.loc[len(df_bd_p)-1,'Type'] = 'Copy'
                #df_bd_p.loc[len(df_bd_p)-1,'Alarma'] = df_data_i.loc[j,'Alarma']
                #df_bd_p.loc[len(df_bd_p)-1,'Operación'] = df_data_i.loc[j,'Operación']
                #df_bd_p.loc[len(df_bd_p)-1,'TXT_Agru'] = df_data_i.loc[j,'TXT_Agru']

                aux = df_bd_p[df_bd_p['Description'] == df_data_i.loc[j,'TXT_Agru']] # Buscar agrupada
                aux = aux.reset_index(drop=True)
                df_bd_p.loc[len(df_bd_p)-1,'SP_Agru'] = aux.loc[0,'System Point']

                j +=1

    print('Base de datos generada')
    return df_bd_p