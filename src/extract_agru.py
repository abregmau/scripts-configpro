### Librerías
import pandas as pd
import numpy as np
from time import time
import csv

### Funciones

def lectura_alar(alar, didesc, micro):
    lista = []
    i=0
    csvarchivo = open(alar)  # Abrir archivo csv
    reg1 = csv.reader(csvarchivo)  # Leer todos los registros

    txtarchivo = open(didesc) # Abrir archivo txt
    reg2 = txtarchivo.readlines() # Leer todos los registros

    for row in reg1:
        operacion, inicio, nro, fechado, a, b = row # Desempaquetar campos registro1
        descripcion = reg2[i].rstrip('\n') # Desempaquetar txt registro2
        inicio = int (inicio) #convierte en valores
        nro = int (nro) #convierte en valores
        #print(row)
        #print(descripcion, micro, i, operacion, inicio, nro, fechado)  # Mostrar campos
        lista.append([ descripcion, micro, i+1, operacion, inicio, nro, fechado ])
        i=i+1
    
    while len(reg2) > i: # Código para agregar a la lista las agrupadas de reserva sin función definida en e423alar
        descripcion = reg2[i].rstrip('\n') # Desempaquetar txt registro2
        #print(descripcion, micro, i, '', '', '', '')  # Mostrar campos
        lista.append([ descripcion, micro, i+1, '', '', '', '' ])
        i=i+1
    
    return lista

def lectura_data(data, micro):
    lista = []
    i=1
    b=0
    c=''
    csvarchivo = open(data)  # Abrir archivo csv
    reg = csv.reader(csvarchivo)  # Leer todos los registros 
    
    for row in reg:
        entrada, alarma, fechado, status, a = row # Desempaquetar campos registro
        syspoint = entrada[1:7] # Extrae systempoint
        if syspoint != '______': # Convierte en valores systempoints
            syspoint = int(syspoint)
        txt = entrada[9:] # Extrae TXT
        #print(syspoint, txt, alarma, fechado, status, micro, indice)
        lista.append([ syspoint, txt, alarma, c, fechado, status, micro, i, b, c ])
        i=i+1
    
    return lista

def escritura_xlsx(exp, df_alar, df_data):

    with pd.ExcelWriter(exp) as writer: # pylint: disable=abstract-class-instantiated
        df_alar.to_excel(writer, sheet_name='ALAR', index=False)
        df_data.to_excel(writer, sheet_name='DATA', index=False)
    
    print("Escritura " + exp + " completa")


def lectura(e423alar, e423data, didesc, micro):

    print('Leyendo archivos agrupadas...')
    
    lista_alar = []
    enc_alar = [ 'Descripción', 'Micro', 'Local Point', 'Operación', 'Inicio', 'Nro. Alarmas', 'Fechado' ] # Encabezado

    lista_data = []
    enc_data = [ 'Syspoint', 'Descripción', 'Alarma', 'Operación', 'Fechado', 'Status', 'Micro', 'Índice', 'LP_Agru', 'TXT_Agru'] # Encabezado

    for m in micro:
        print('Leyendo micro:',m)
        j = micro.index(m)
        lista_alar.extend(lectura_alar(e423alar[j], didesc[j], micro[j])) # Levantar en una lista todas las agrupadas de todos los micros
        lista_data.extend(lectura_data(e423data[j], micro[j])) # Levantar en una lista todas las entradas agrupadas de todos los micros

    print('Procesando Agrupamientos...')
    for a in lista_alar: # Doble for para completar LP y TXT y Operación de agrupada a la que pertence en tabla DATA
        i=0
        for d in lista_data:
            if a[4] != '': # No comparar las agrupadas de reserva
                if d[6] == a[1] and d[7] >= a[4] and d[7] < a[4] + a[5]:
                    lista_data[i][3] = a[3]
                    lista_data[i][8] = a[2]
                    lista_data[i][9] = a[0]
            i=i+1
    
    df_alar = pd.DataFrame(data=lista_alar, columns=enc_alar)
    df_data = pd.DataFrame(data=lista_data, columns=enc_data)

    print('Agrupamientos procesados con éxito')
    return df_alar, df_data