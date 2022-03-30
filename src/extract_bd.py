### Librerías

import csv

### Definición de constantes y variables

dispositivo = 'CN-62'

bd = 'input/RTUs/' + dispositivo + '/' + dispositivo + '.csv' # Dirección y nombre base de datos origen
bd_exp = 'output/TABLAS/' + dispositivo + '/' + dispositivo + '_exp.csv' # Dirección y nombre base de datos destino

### Funciones

def lectura(bd):
    lista = []
    csvarchivo = open(bd)  # Abrir archivo csv
    reg = csv.reader(csvarchivo)  # Leer todos los registros

    for row in reg:
        point_type, system_point, polarity, local_point, type_, description, block, board, tag_1, tag_2, tag_3 = row # Desempaquetar campos
        #print(row) #Muestra todo lo leído
        if point_type != "Point Type":
            system_point = int (system_point) #convierte en valores
            local_point = int (local_point) #convierte en valores
        if description: # descarta las celdas description vacías
            #print(point_type, system_point, local_point, description)  # Mostrar campos
            lista.append([point_type, system_point, local_point, type_, description, board ])
    
    return lista

def escritura(bd, lista):
    file = open(bd, 'w', newline="")
    with file:
        writer = csv.writer(file)
        writer.writerows(lista)
    print("Escritura completa")

### Código

lista_bd = lectura(bd) # Levantar en una lista la base de datos completa y filtrada
escritura(bd_exp, lista_bd) # Escribir .csv con base de datos

#for row in lista_bd:
#    print(row)

### Fin Código