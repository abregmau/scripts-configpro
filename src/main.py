################################ Librerías
import pandas as pd
import numpy as np
from time import time
import os
import shutil

### Librerías propias
import extract_bd as ebd
import extract_agru as e_agru
import gen_agru as g_agru

################################ Constantes

################################ Funciones

def constantes(df_datos, disp):
	# Definición de constantes de usuario globales
	global dispositivo
	global micro
	global m_cot
	global dir_rtu

	dispositivo = df_datos.loc[disp,'Dispositivo'] # Nombre dispositivo

	micro = [] # Define los números de micro donde se encuentran los agrupamientos
	if df_datos.loc[disp,'Agru M1'] == 'si':
		micro.append(1)
	if df_datos.loc[disp,'Agru M2'] == 'si':
		micro.append(2)
	if df_datos.loc[disp,'Agru M3'] == 'si':
		micro.append(3)

	dire = df_datos.loc[disp,'Dirección proyecto']
	m_cot = int(df_datos.loc[disp,'Micro DNP COT'])
	dir_rtu =int(df_datos.loc[disp, 'Dir RTU DNP COT'])

	# Definición de constantes globales del programa

	# Origen de datos
	global bd
	global e423alar
	global e423data
	global didesc
	global cot
	global cot_b021
	global agru_exp_xlsx

	bd = dire + '\\' + dispositivo + '\\' + dispositivo + '.csv' # Dirección y nombre base de datos origen
	
	e423alar = []
	e423data = []
	didesc = []
	for row in micro: # Creación de lista de rutas origen datos agrupadas
		e423alar.append ( dire + '\\' + dispositivo + '\\e423.' + str(row-1) + '\\e423alar.csv')
		e423data.append ( dire + '\\' + dispositivo + '\\e423.' + str(row-1) + '\\e423data.csv')
		didesc.append ( dire + '\\' + dispositivo + '\\e423.' + str(row-1) + '\\didesc.txt')

	b021_cfg = dire + '\\' + dispositivo + '\\b021.' + str(m_cot-1) + '\\B021CFG.csv'
	b021_mt01 = dire + '\\' + dispositivo + '\\b021.' + str(m_cot-1) + '\\B021MT01.csv'
	b021_mt02 = dire + '\\' + dispositivo + '\\b021.' + str(m_cot-1) + '\\B021MT02.csv'
	b021_mt04 = dire + '\\' + dispositivo + '\\b021.' + str(m_cot-1) + '\\B021MT04.csv'
	cot_b021 = [b021_cfg, b021_mt01, b021_mt02, b021_mt04]

	cot = 'input\\COT\\' + df_datos.loc[disp, 'COT'] + '.xlsx' # Dirección y nombre datos COT
	agru_exp_xlsx = 'input\\AGRU\\' + dispositivo + '\\' + dispositivo + '_AGRU.xlsx' # Dirección y nombre base de datos de origen para generación de agrupadas

	#Destino de datos
	global bd_exp
	global cot_exp
	global agru_exp
	global e423alar_exp
	global e423data_exp
	global didesc_exp
	bd_exp = 'output\\TABLAS\\' + dispositivo + '\\' + dispositivo + '_EXP.xlsx' # Dirección y nombre base completa a exportar
	cot_exp = 'output\\TABLAS\\' + dispositivo + '\\' + dispositivo + '_COT_EXP.xlsx' # Dirección y nombre base agrupadas a exportar
	agru_exp = 'output\\TABLAS\\' + dispositivo + '\\' + dispositivo + '_AGRU.xlsx' # Dirección y nombre base agrupadas a exportar
	os.makedirs('output\\TABLAS\\' + dispositivo , exist_ok=True)

	e423alar_exp = []
	e423data_exp = []
	didesc_exp = []
	for row in micro: # Creación de lista de rutas para exportar
		e423alar_exp.append ('output\\AGRU_MOD\\' + dispositivo + '\\e423.' + str(row-1) + '/e423alar.csv')
		e423data_exp.append ('output\\AGRU_MOD\\' + dispositivo + '\\e423.' + str(row-1) + '/e423data.csv')
		didesc_exp.append ('output\\AGRU_MOD\\' + dispositivo + '\\e423.' + str(row-1) + '/didesc.txt')
		os.makedirs('output\\AGRU_MOD\\' + dispositivo + '\\e423.' + str(row-1), exist_ok=True)


def menu():
	## Función que limpia la pantalla y muestra nuevamente el menu
	os.system('cls') # NOTA para windows tienes que cambiar clear por cls
	print('BASE DE DATOS RTUs - ULTRA V0.2')
	print('Desarrollador Abregú Mauricio\n\n')
	print ('Selecciona una opción')
	print ('\t1 - Seleccionar Dispositivo')
	print ('\t2 - Crear Base de datos con Agrupamientos')
	print ('\t3 - Crear Base de datos con Agrupamientos + Puntos COT')
	print ('\t4 - Archivo COT')
	print ('\t5 - Archivo AGRU')
	print ('\t6 - Crear Base de datos opción 3 + 4 + 5 en un archivo')
	print ('\t7 - Generar .csv Agrupadas a partir de archivo AGRU')


	print ('\t9 - Salir')

	opcion = input('Escriba una opción\n')
	return opcion

def select_disp(df_datos):
	## Función que limpia la pantalla y muestra nuevamente el menu
	os.system('cls') # NOTA para windows tienes que cambiar clear por cls
	print(df_datos[['Dispositivo', 'Dirección proyecto']])
	disp = input('Escriba una opción\n')
	while disp not in str(df_datos.index.values):
		disp = input('Opción no válida, escriba una opción\n') 

	return int(disp)

def datos():
	# Leer pestaña DATOS
	file = pd.read_excel('input\\Dispositivos.xlsx', sheet_name='DATOS')
	df_datos = pd.DataFrame(file)

	return df_datos

################################ Programa
df_datos = datos()
disp = select_disp(df_datos)
constantes(df_datos, disp)

while True:
	# Mostramos el menu
	opcion = menu()
	if opcion == "1":
		print ("")
		disp = select_disp(df_datos)
		constantes(df_datos, disp)

	elif opcion == "2":
		print ("")
		### Lecturas
		df_bd = ebd.lectura(bd) # Levantar base de datos completa y filtrada
		df_alar, df_data = e_agru.lectura(e423alar, e423data, didesc, micro) # Levantar todos los agrupamientos
		### Procesamiento
		df_bd_p = ebd.procesa_agru(df_bd, df_data)
		### Escrituras
		ebd.escritura_bd_xlsx(bd_exp, df_bd_p, dispositivo) # Escribir .xlsx con base de datos

		input("Operación completa!\nPulsa una tecla para continuar...")

	elif opcion == "3":
		print ("")
		### Lecturas
		df_bd = ebd.lectura(bd) # Levantar base de datos completa y filtrada
		df_alar, df_data = e_agru.lectura(e423alar, e423data, didesc, micro) # Levantar todos los agrupamientos
		df_b021_mt01, df_b021_mt02, df_b021_mt04 = ebd.lectura_dpa(cot_b021, dir_rtu)
		df_cot_di, df_cot_ai, df_cot_do = ebd.lectura_cot(cot, df_b021_mt01, df_b021_mt02, df_b021_mt04)
		### Procesamiento
		df_bd_p = ebd.procesa_agru(df_bd, df_data)
		df_bd_p = ebd.procesa_cot(df_bd, df_bd_p, df_cot_di, df_cot_ai, df_cot_do)
		### Escrituras
		ebd.escritura_bd_xlsx(bd_exp, df_bd_p, dispositivo) # Escribir .xlsx con base de datos

		input("Operación completa!\nPulsa una tecla para continuar...")

	elif opcion == "4":
		print ("")
		### Lecturas
		df_b021_mt01, df_b021_mt02, df_b021_mt04 = ebd.lectura_dpa(cot_b021, dir_rtu)
		df_cot_di, df_cot_ai, df_cot_do = ebd.lectura_cot(cot, df_b021_mt01, df_b021_mt02, df_b021_mt04)
		### Escrituras
		ebd.escritura_dnp_xlsx(df_cot_di, df_cot_ai, df_cot_do, cot_exp) # Escribir .xlsx con base de datos DNP

		input("Operación completa!\nPulsa una tecla para continuar...")

	elif opcion == "5":
		print ("")
		### Lecturas
		df_alar, df_data = e_agru.lectura(e423alar, e423data, didesc, micro) # Levantar todos los agrupamientos
		### Escrituras
		e_agru.escritura_xlsx(agru_exp, df_alar, df_data)

		input("Operación completa!\nPulsa una tecla para continuar...")

	elif opcion == "6":
		print ("")

		### Lecturas
		df_bd = ebd.lectura(bd) # Levantar base de datos completa y filtrada
		df_alar, df_data = e_agru.lectura(e423alar, e423data, didesc, micro) # Levantar todos los agrupamientos
		df_b021_mt01, df_b021_mt02, df_b021_mt04 = ebd.lectura_dpa(cot_b021, dir_rtu)
		df_cot_di, df_cot_ai, df_cot_do = ebd.lectura_cot(cot, df_b021_mt01, df_b021_mt02, df_b021_mt04)
		### Procesamiento
		df_bd_p = ebd.procesa_agru(df_bd, df_data)
		df_bd_p = ebd.procesa_cot(df_bd, df_bd_p, df_cot_di, df_cot_ai, df_cot_do)
		### Escrituras
		ebd.escritura_bd_xlsx(bd_exp, df_bd_p, dispositivo) # Escribir .xlsx con base de datos
		ebd.escritura_dnp_xlsx(df_cot_di, df_cot_ai, df_cot_do, cot_exp) # Escribir .xlsx con base de datos DNP
		e_agru.escritura_xlsx(agru_exp, df_alar, df_data) # Escribir .xlsx con base de datos Agrupadas

		### Combinacion de archivos
		print('Combinando Archivos')
		file = pd.read_excel(bd_exp, sheet_name=dispositivo)
		df_base = pd.DataFrame(file)
		file = pd.read_excel(agru_exp, sheet_name='ALAR')
		df_base_agru_alar = pd.DataFrame(file)
		file = pd.read_excel(agru_exp, sheet_name='DATA')
		df_base_agru_data = pd.DataFrame(file)
		file = pd.read_excel(cot_exp, sheet_name='DI')
		df_base_cot_di = pd.DataFrame(file)
		file = pd.read_excel(cot_exp, sheet_name='AI')
		df_base_cot_ai = pd.DataFrame(file)
		file = pd.read_excel(cot_exp, sheet_name='DO')
		df_base_cot_do = pd.DataFrame(file)

		with pd.ExcelWriter(bd_exp) as writer: # pylint: disable=abstract-class-instantiated
			df_base.to_excel(writer, sheet_name= dispositivo, index=False)
			df_base_agru_alar.to_excel(writer, sheet_name= 'AGRU-ALAR', index=False)
			df_base_agru_data.to_excel(writer, sheet_name= 'AGRU-DATA', index=False)
			df_base_cot_di.to_excel(writer, sheet_name= 'COT-DI', index=False)
			df_base_cot_ai.to_excel(writer, sheet_name= 'COT-AI', index=False)
			df_base_cot_do.to_excel(writer, sheet_name= 'COT-DO', index=False)



		input("Operación completa!\nPulsa una tecla para continuar...")

	elif opcion == "7":
		print ("")
		start_time = time() # Star Medición de tiempo
		### Lecturas
		df_alar, df_data = g_agru.leer_conf(agru_exp_xlsx) # Lectura de configuración a generar desde archivo AGRU.xlsx
		### Procesamiento
		for m in micro:
			j = micro.index(m)
			df_alar_exp, df_data_exp, df_descripcion = g_agru.procesa_agru(df_alar, df_data, m) # Llamdo a proceso
			df_alar_exp.to_csv(e423alar_exp[j], index=False, header=False) # Escirbir .csv ALAR
			df_data_exp.to_csv(e423data_exp[j], index=False, header=False) # Escribir .csv DATA
			df_descripcion.to_csv(didesc_exp[j], index=False, header=False) # Escribir .txt
		elapsed_time = time() - start_time
		print("Código Ejecutado con exito, tiempo de proceso: {0:.3}".format(elapsed_time))

		input("Operación completa!\nPulsa una tecla para continuar...")

	elif opcion == "9":
		print ("")
		break
