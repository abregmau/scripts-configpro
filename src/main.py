################################ Librerías
import pandas as pd
from pandas import ExcelWriter
import numpy as np
from time import time

### Librerías propias
import extract_bd as ebd
import extract_agru as eagru

################################ Constantes

### Definición de constantes de usuario
dispositivo = 'CN-62'
micro = [ 1, 2, 3] # Define los números de micro donde se encuentran los agrupamientos

### Constantes del programa
# Origen
bd = 'input/RTUs/' + dispositivo + '/' + dispositivo + '.csv' # Dirección y nombre base de datos origen
e423alar = []
e423data = []
didesc = []

for row in micro: # Creación de lista de rutas origen datos agrupadas
    e423alar.append ( 'input/RTUs/' + dispositivo + '/e423.' + str(row-1) + '/e423alar.csv')
    e423data.append ( 'input/RTUs/' + dispositivo + '/e423.' + str(row-1) + '/e423data.csv')
    didesc.append ( 'input/RTUs/' + dispositivo + '/e423.' + str(row-1) + '/didesc.txt')

#Destino
bd_exp = 'output/TABLAS/' + dispositivo + '/' + dispositivo + '_EXP.xlsx' # Dirección y nombre base completa a exportar
agru_exp = 'output/TABLAS/' + dispositivo + '/' + dispositivo + '_AGRU.xlsx' # Dirección y nombre base agrupadas a exportar

################################ Programa

### Lecturas
df_bd = ebd.lectura(bd) # Levantar base de datos completa y filtrada
df_alar, df_data = eagru.lectura(e423alar, e423data, didesc, micro) # Levantar todos los agrupamientos

### Procesamiento
df_bd_p=ebd.procesa(df_bd, df_data)

### Escrituras
ebd.escritura_xlsx(bd_exp, df_bd_p, dispositivo) # Escribir .xlsx con base de datos
eagru.escritura_xlsx(agru_exp, df_alar, df_data)