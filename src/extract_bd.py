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

def escritura_bd_xlsx(dir, df, sheet):

    with pd.ExcelWriter(dir) as writer: # pylint: disable=abstract-class-instantiated
        df.to_excel(writer, sheet_name= sheet, index=False)
    
    print('Escritura ' + dir + ' completa')

def escritura_dnp_xlsx(df_di, df_ai, df_do, dir):

    with pd.ExcelWriter(dir) as writer: # pylint: disable=abstract-class-instantiated
        df_di.to_excel(writer, sheet_name= 'DI', index=False)
        df_ai.to_excel(writer, sheet_name= 'AI', index=False)
        df_do.to_excel(writer, sheet_name= 'DO', index=False)
    
    print('Escritura ' + dir + ' completa')

def lectura_dpa(b021, dir):

    b021_cfg = b021[0]
    b021_mt01 = b021[1]
    b021_mt02 = b021[2]
    b021_mt04 = b021[3]

    # Leer B021CFG
    csvarchivo = open(b021_cfg)  # Abrir archivo csv
    reg = csv.reader(csvarchivo)  # Leer todos los registros 

    for row in reg:
        dnp_rtu, dnp_cot, di_tot, di_ini, do_tot, do_ini, a, b, c, d, ai_tot, ai_ini = row[0:12]
        dnp_rtu = int(dnp_rtu)
        if dnp_rtu == dir:
            di_tot = int(di_tot)
            di_ini = int(di_ini)
            do_tot = int(do_tot)
            do_ini = int(do_ini)
            ai_tot = int(ai_tot)
            ai_ini = int(ai_ini)
            break

    # Leer B021MT01
    lista_b021_mt01 = []
    i=1
    j=1
    csvarchivo = open(b021_mt01)  # Abrir archivo csv
    reg = csv.reader(csvarchivo)  # Leer todos los registros 
    
    for row in reg:
        if i >= di_ini and i < di_ini + di_tot:
            entrada, invert, cos, soe, eventclass = row # Desempaquetar campos registro
            syspoint = entrada[1:7] # Extrae systempoint
            if syspoint != '______': # Convierte en valores systempoints
                syspoint = int(syspoint)
            txt = entrada[9:] # Extrae TXT
            lista_b021_mt01.append([ j, syspoint, txt, invert, cos, soe, eventclass])
            j +=1
        i +=1

    enc_b021_mt01 = ['DNP', 'Syspoint', 'Description', 'Invert Status', 'COS', 'SOE', 'Event Class']
    df_b021_mt01 = pd.DataFrame(data=lista_b021_mt01, columns=enc_b021_mt01)
    
    # Leer B021MT02
    lista_b021_mt02 = []
    i=1
    j=1
    csvarchivo = open(b021_mt02)  # Abrir archivo csv
    reg = csv.reader(csvarchivo)  # Leer todos los registros 
    
    for row in reg:
        if i >= do_ini and i < do_ini + do_tot:
            entrada, paired = row # Desempaquetar campos registro
            syspoint = entrada[1:7] # Extrae systempoint
            if syspoint != '______': # Convierte en valores systempoints
                syspoint = int(syspoint)
            txt = entrada[9:] # Extrae TXT
            lista_b021_mt02.append([ j, syspoint, txt])
            j +=1
        i +=1

    enc_b021_mt02 = ['DNP', 'Syspoint', 'Description']
    df_b021_mt02 = pd.DataFrame(data=lista_b021_mt02, columns=enc_b021_mt02)

    # Leer B021MT04
    lista_b021_mt04 = []
    i=1
    j=1
    csvarchivo = open(b021_mt04)  # Abrir archivo csv
    reg = csv.reader(csvarchivo)  # Leer todos los registros 
    
    for row in reg:
        if i >= ai_ini and i < ai_ini + ai_tot:
            entrada = row[0] # Desempaquetar campos registro
            syspoint = entrada[1:7] # Extrae systempoint
            if syspoint != '______': # Convierte en valores systempoints
                syspoint = int(syspoint)
            txt = entrada[9:] # Extrae TXT
            lista_b021_mt04.append([ j, syspoint, txt])
            j +=1
        i +=1

    enc_b021_mt04 = ['DNP', 'Syspoint', 'Description']
    df_b021_mt04 = pd.DataFrame(data=lista_b021_mt04, columns=enc_b021_mt04)

    return df_b021_mt01, df_b021_mt02, df_b021_mt04

def lectura_cot(cot, df_b021_mt01, df_b021_mt02, df_b021_mt04):

    print('Leyendo datos COT', cot)

    # Leer archivo COT pestaña DI
    file = pd.read_excel(cot, sheet_name='DI')
    df_cot_di = pd.DataFrame(file)
    df_cot_di = df_cot_di[['Name', 'FEP Key', 'Point Address']]
    df_cot_di['FEP Key'] = df_cot_di['FEP Key'].astype(str)

    # Combinar datos COT con B021MT01
    df_cot_di_p = pd.merge(left= df_b021_mt01, right= df_cot_di, how='outer', left_on= 'DNP', right_on= 'Point Address')
    df_cot_di_p = df_cot_di_p.drop('Point Address', axis= 1)
    #df_cot_di_p = df_cot_di_p.drop('COS', axis= 1)
    #df_cot_di_p = df_cot_di_p.drop('SOE', axis= 1)
    #df_cot_di_p = df_cot_di_p.drop('Event Class', axis= 1)

    # Leer archivo COT pestaña AI
    file = pd.read_excel(cot, sheet_name='AI')
    df_cot_ai = pd.DataFrame(file)
    df_cot_ai = df_cot_ai[['Name', 'FEP Key', 'Point Address']]
    df_cot_ai['FEP Key'] = df_cot_ai['FEP Key'].astype(str)

    # Combinar datos COT con B021MT04
    df_cot_ai_p = pd.merge(left= df_b021_mt04, right= df_cot_ai, how='outer', left_on= 'DNP', right_on= 'Point Address')
    df_cot_ai_p = df_cot_ai_p.drop('Point Address', axis= 1)

    # Leer archivo COT pestaña DO
    file = pd.read_excel(cot, sheet_name='DO')
    df_cot_do = pd.DataFrame(file)
    df_cot_do = df_cot_do[['SCADA Key', 'Point Address', 'Control Type', 'Default Execute Ticks', 'Execute Multiplier']]
    df_cot_do['SCADA Key'] = df_cot_do['SCADA Key'].astype(str)

    # Combinar datos COT con B021MT02
    df_cot_do_p = pd.merge(left= df_b021_mt02, right= df_cot_do, how='outer', left_on= 'DNP', right_on= 'Point Address')
    df_cot_do_p = df_cot_do_p.drop('Point Address', axis= 1)
    df_cot_do_p = pd.merge(left= df_cot_do_p, right= df_cot_di_p[['Name','FEP Key']], how='left', left_on= 'SCADA Key', right_on= 'FEP Key')
    df_cot_do_p = df_cot_do_p.drop('FEP Key', axis= 1)
    df_cot_do_p = df_cot_do_p.drop_duplicates()

    return df_cot_di_p, df_cot_ai_p, df_cot_do_p

def procesa_agru(df_bd, df_data):
 
    print('Procesando archivos, unificando con agrupadas...')
    
    #df_bd_p = Base de datos procesada!

    # Variables Auxiliares
    df_bd_p = df_bd[df_bd['Point Type'] == 'DI'] # DI a procesar
    df_bd_aux1 = df_bd[df_bd['Point Type'] != 'DI'] # Parte de BD que no contiene DI
    df_bd_aux2 = df_bd_p[['System Point', 'Local Point', 'Description']] # Para procesar nombres agrupamientos
    df_data_aux = df_data[['Syspoint', 'Alarma', 'Operación', 'Micro', 'TXT_Agru', 'LP_Agru']] # Data filtrado por columnas necesarias

    # Combinar BD con data
    df_bd_p = pd.merge(left= df_bd_p, right= df_data_aux, how='left', left_on= 'System Point', right_on= 'Syspoint')
    # Buscar nombres de agrupadas
    df_bd_p = pd.merge(left= df_bd_p, right= df_bd_aux2, how='left', left_on= ['TXT_Agru', 'LP_Agru'], right_on= ['Description', 'Local Point'], suffixes=('', '_1'))
    # Limpiar columnas
    df_bd_p = df_bd_p.drop(['Syspoint', 'LP_Agru', 'Local Point_1', 'Description_1'], axis= 1)
    df_bd_p = df_bd_p.rename(columns={'System Point_1': 'SP_Agru_1'})

    # Recombinar BD con DATA en busca de agrupamientos anidados
    df_bd_p = pd.merge(left= df_bd_p, right= df_data_aux, how='left', left_on= 'SP_Agru_1', right_on= 'Syspoint', suffixes=('', '_2'))
    # Buscar nombres de agrupadas anidadas
    df_bd_p = pd.merge(left= df_bd_p, right= df_bd_aux2, how='left', left_on= ['TXT_Agru_2', 'LP_Agru'], right_on= ['Description', 'Local Point'], suffixes=('', '_2'))
    # Limpiar columnas
    df_bd_p = df_bd_p.drop(['Syspoint', 'Alarma_2', 'Operación_2', 'Micro_2', 'LP_Agru', 'Local Point_2', 'Description_2'], axis= 1)
    df_bd_p = df_bd_p.rename(columns={'System Point_2': 'SP_Agru_2', 'TXT_Agru': 'TXT_Agru_1'})

    df_bd_p_1 = df_bd_p[df_bd_p['TXT_Agru_2'].isna()] # BD procesado Parte 1, puntos no agrupados y puntos en agrupadas no anidadas
    df_bd_p_2 = df_bd_p[~df_bd_p['TXT_Agru_2'].isna()] # BD procesado Parte 2, puntos en agrupadas anidadas
    # Copia de agrupamientos para tabla dinámica excel
    df_bd_p_1 = df_bd_p_1.assign(TXT_Agru_2=df_bd_p_1['TXT_Agru_1'])
    df_bd_p_1 = df_bd_p_1.assign(SP_Agru_2=df_bd_p_1['SP_Agru_1'])

    # Regenerar bd
    df_bd_p = pd.concat([ df_bd_p_1, df_bd_p_2, df_bd_aux1], sort= False)
    df_bd_p = df_bd_p.sort_values(['Point Type', 'System Point'])
    df_bd_p = df_bd_p.reset_index(drop= True)

    print('Base de datos generada')
    return df_bd_p

def procesa_cot(df_bd, df_bd_p, df_cot_di, df_cot_ai, df_cot_do):
    
    print('Procesando archivos, unificando con datos COT...')

    # Variables Auxiliares
    df_bd_aux1 = df_bd[(df_bd['Point Type'] != 'DI') & (df_bd['Point Type'] != 'AI') & (df_bd['Point Type'] != 'DO')]
    df_bd_di = df_bd[df_bd['Point Type'] == 'DI']
    df_bd_ai = df_bd[df_bd['Point Type'] == 'AI']
    df_bd_do = df_bd[df_bd['Point Type'] == 'DO']
    df_bd_p = df_bd_p[ df_bd_p['Point Type'] == 'DI']

    df_cot_di = df_cot_di[['Syspoint', 'Invert Status', 'Name', 'FEP Key']]
    df_cot_di = df_cot_di.dropna(subset=['Syspoint'])

    df_cot_ai = df_cot_ai[['Syspoint', 'Name', 'FEP Key']]
    df_cot_ai = df_cot_ai.dropna(subset=['Syspoint'])

    df_cot_do = df_cot_do[['Syspoint', 'Name', 'SCADA Key']]
    df_cot_do = df_cot_do.rename(columns={'SCADA Key':'FEP Key'})
    df_cot_do = df_cot_do.dropna(subset=['Syspoint'])
    df_cot_do = df_cot_do.drop_duplicates()

    # Combinar bd original DI individuales con COT (resultado: Puntos a COT directos)
    df_bd_indv = pd.merge(left= df_bd_di, right= df_cot_di, how='inner', left_on= 'System Point', right_on= 'Syspoint')
    df_bd_indv = df_bd_indv.drop('Syspoint', axis= 1)

    # Combinar bd agrupamientos con COT (resultado: Puntos que van a COT por agrupamiento)
    df_bd_agru = pd.merge(left= df_bd_p, right= df_cot_di, how='left', left_on= 'SP_Agru_2', right_on= 'Syspoint')
    df_bd_agru = df_bd_agru.drop('Syspoint', axis= 1)
    df_bd_agru = df_bd_agru[ ~df_bd_agru['SP_Agru_2'].isna()]

    # Buscar puntos que no van a COT de forma individual o que no forman parte de agrupada (resultado: Puntos no COT y no agrupados)
    df_bd_not = df_bd_p[~df_bd_p['System Point'].isin(df_bd_indv['System Point'])]
    df_bd_not = df_bd_not[~df_bd_not['System Point'].isin(df_bd_agru['System Point'])]

    # Combinar bd original AI con COT
    df_bd_ai = pd.merge(left= df_bd_ai, right= df_cot_ai, how='left', left_on= 'System Point', right_on= 'Syspoint')
    df_bd_ai = df_bd_ai.drop('Syspoint', axis= 1)

    # Combinar bd original DO con COT
    df_bd_do = pd.merge(left= df_bd_do, right= df_cot_do, how='left', left_on= 'System Point', right_on= 'Syspoint')
    df_bd_do = df_bd_do.drop('Syspoint', axis= 1)

    # Regenerar bd final
    df_bd_f = pd.concat([ df_bd_not, df_bd_agru, df_bd_indv, df_bd_aux1, df_bd_ai, df_bd_do ], sort= False)
    df_bd_f = df_bd_f.sort_values(['Point Type', 'System Point'])
    df_bd_f = df_bd_f.reset_index(drop= True)

    print('Base de datos generada')
    return df_bd_f