### Librerías

import csv
import pandas as pd
from pandas import ExcelWriter


### Definición de constantes y variables

dispositivo = 'CN-62'

micro = [ 1, 2, 3] # Define los números de micro donde se encuentran los agrupamientos

alar_exp = 'output/TABLAS/' + dispositivo + '/' + dispositivo + '_alar.csv' # Dirección y nombre base de datos destino
data_exp = 'output/TABLAS/' + dispositivo + '/' + dispositivo + '_data.csv' # Dirección y nombre base de datos destino

agru_exp_xlsx = 'output/TABLAS/' + dispositivo + '/' + dispositivo + '_AGRU.xlsx' # Dirección y nombre base de datos destino

e423alar = []
e423data = []
didesc = []

for row in micro: # Creación de lista de rutas
    e423alar.append ( 'input/RTUs/' + dispositivo + '/e423.' + str(row-1) + '/e423alar.csv')
    e423data.append ( 'input/RTUs/' + dispositivo + '/e423.' + str(row-1) + '/e423data.csv')
    didesc.append ( 'input/RTUs/' + dispositivo + '/e423.' + str(row-1) + '/didesc.txt')


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

def escritura(bd, lista, encabezado):

    lista.insert(0, encabezado)

    file = open(bd, 'w', newline="")
    with file:
        writer = csv.writer(file)
        writer.writerows(lista)
    print("Escritura " + bd + " completa")

def escritura_xlsx(bd, alar, enc_alar, data, enc_data):

    df_alar = pd.DataFrame(data=alar, columns=enc_alar)
    df_data = pd.DataFrame(data=data, columns=enc_data)

    with ExcelWriter(bd) as writer: # pylint: disable=abstract-class-instantiated
        df_alar.to_excel(writer, sheet_name='ALAR', index=False)
        df_data.to_excel(writer, sheet_name='DATA', index=False)
    
    print("Escritura " + bd + " completa")

### Código

lista_alar = []
enc_alar = [ 'Descripción', 'Micro', 'Local Point', 'Operación', 'Inicio', 'Nro. Alarmas', 'Fechado' ] # Encabezado

lista_data = []
enc_data = [ 'Syspoint', 'Descripción', 'Alarma', 'Operacion', 'Fechado', 'Status', 'Micro', 'Índice', 'LP_Agru', 'TXT_Agru'] # Encabezado

for m in micro:
    lista_alar.extend(lectura_alar(e423alar[m-1], didesc[m-1], micro[m-1])) # Levantar en una lista todas las agrupadas de todos los micros
    lista_data.extend(lectura_data(e423data[m-1], micro[m-1])) # Levantar en una lista todas las entradas agrupadas de todos los micros

for a in lista_alar: # Doble for para completar LP y TXT y Operación de agrupada a la que pertence en tabla DATA
    i=0
    for d in lista_data:
        if a[4] != '': # No comparar las agrupadas de reserva
            if d[6] == a[1] and d[7] >= a[4] and d[7] < a[4] + a[5]:
                lista_data[i][3] = a[3]
                lista_data[i][8] = a[2]
                lista_data[i][9] = a[0]
        i=i+1


#escritura(alar_exp, lista_alar[:], enc_alar) # Escribir .csv con base de datos
#escritura(data_exp, lista_data[:], enc_data) # Escribir .csv con base de datos

escritura_xlsx(agru_exp_xlsx, lista_alar[:], enc_alar, lista_data[:], enc_data) # Escribir .xlsx con base de datos

### Fin Código