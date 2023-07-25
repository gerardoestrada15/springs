#!/usr/bin/python3
import sqlite3
import requests
import time
import pandas as pd
from datetime import date
from datetime import datetime

API_ENDPOINT ="https://swfoee.goandsee.co/api/v3/oee"
###################################
#### Funciones auxiliares
#### Convertir un arreglo a un objeto
def to_remote_object(databaseRecord):
  temp_object = {}
  temp_object['date'] = databaseRecord[0]
  temp_object['sensor'] = databaseRecord[1]
  temp_object['eth_mac'] = databaseRecord[2]
  temp_object['date_time'] = databaseRecord[3]
  return #temp_object
###################################
def objeto_remoto(databaseRecord):
  temp_object = {}
  
  temp_object['date'] = databaseRecord[0]
  temp_object['sensor'] = databaseRecord[1]
  temp_object['eth_mac'] = databaseRecord[2]
  temp_object['date_time'] = databaseRecord[3]
  return temp_object


maximo_renglones=6000

### Conectar  ala base de datos
limitadorTemporal_A=time.time()

tiempoA=time.time()
connection = sqlite3.connect('/home/pi/Desktop/hibrido/colector_X.db')
cursor = connection.cursor()
congelada = pd.read_sql_query("SELECT * FROM oeerecords ORDER BY date", connection)
connection.close()
tiempoB=time.time()

print("duracion LEYENDO base de datos", tiempoB - tiempoA)

congelada[["date"]] = congelada[["date"]].apply(pd.to_numeric)
congelada = congelada.sort_values(by=["date"], ascending=True)
print("congelada")
print(congelada)

if len(congelada)>0:
    max_date_congelada = congelada.loc[congelada["date"].idxmax()]
    max_date_congelada=max_date_congelada[0]
    #print("max_date_congelada", max_date_congelada)
     
    n_renglones=len(congelada)
    
    if n_renglones>maximo_renglones:    
        n_renglones=maximo_renglones
    
    #print("congelada n+1")
    #print(congelada.iloc[0:n_renglones+1,:])
    
    pimer_tanda_max_date = congelada.iloc[n_renglones-1,0]
    #print("pimer_tanda_max_date",pimer_tanda_max_date)
    
    date_col = congelada["date"][0:n_renglones]
    sensor_col = congelada["sensor"][0:n_renglones].astype(str)
    eth_mac_col = congelada["eth_mac"][0:n_renglones].astype(str)
    fecha_col = congelada["fecha"][0:n_renglones].astype(str)
    
    resultado = pd.concat([date_col, sensor_col], axis=1)
    resultado = pd.concat([resultado, eth_mac_col], axis=1)
    resultado = pd.concat([resultado, fecha_col], axis=1)
        
    objeto_resultado = resultado.to_records(index=False)    
    lista_resultado=objeto_resultado.tolist()
    dataToSend_resultado = list(map(objeto_remoto, lista_resultado))

else:
    n_renglones=0
    print("no hay datos")
    limitadorTemporal_Total=3.30303030303030303

print("CONTEO A: renglones ANTES",n_renglones)

if n_renglones>0:
    post_request = requests.post(url = API_ENDPOINT, json = dataToSend_resultado)
    #print("post request",post_request.status_code)

    if post_request.status_code == 200:
        print("primer subida exitosa, conectando a colector por segunda vez para borrar lo que se envio")
        tiempoA=time.time()
        connection = sqlite3.connect('/home/pi/Desktop/hibrido/colector_X.db')
        cursor = connection.cursor()
    
        instruccion = "DELETE FROM oeerecords WHERE date < "
        ptmd_instr = pimer_tanda_max_date+1
        instruccion = instruccion+str(ptmd_instr)
        cursor.execute(instruccion);    
        connection.commit()
        connection.close()
        tiempoB=time.time()
        duracion=tiempoB-tiempoA
        tiempoB=time.time()

        #print("duracion BORRANDO base de datos", tiempoB - tiempoA)
        
        nowe = datetime.now()
        fecha = nowe.strftime("%m/%d/%Y, %H:%M:%S")
        informacion = "".join(str(dataToSend_resultado) for k in dataToSend_resultado)
        pulsos=[(fecha, time.time(),str(n_renglones), informacion)]
        conn = sqlite3.connect('/home/pi/Desktop/hibrido/colector_Y.db')
        c = conn.cursor()
        c.executemany("INSERT INTO  oeerecords VALUES(?,?,?,?)",pulsos)
        conn.commit()
        conn.close()
        
        
        
        
        
        congelada = congelada.drop(congelada[congelada.date < ptmd_instr].index)
        congelada = congelada.reset_index(drop=True)
        
        #print("duracion SECTOR B --BORRANDO---------------------- base de datos abierta", duracion)

        n_renglones=len(congelada)
            

        print("CONTEO A: renglones DESPUES",n_renglones) 
#    print("duracion SECTOR B---------------------- base de datos abierta", duracion)
        limitadorTemporal_B=time.time()
        limitadorTemporal_Total=limitadorTemporal_B-limitadorTemporal_A

        while (n_renglones>0) and (limitadorTemporal_Total<10):

            if n_renglones>maximo_renglones:    
                n_renglones=maximo_renglones
           
            #print("congelada B antes")
            #print(congelada.iloc[0:n_renglones+1,:])
            segunda_tanda_max_date = congelada.iloc[n_renglones-1,0]
#            print("segunda_tanda_max_date",segunda_tanda_max_date)
    
            date_col = congelada["date"][0:n_renglones]
            sensor_col = congelada["sensor"][0:n_renglones].astype(str)
            eth_mac_col = congelada["eth_mac"][0:n_renglones].astype(str)
            fecha_col = congelada["fecha"][0:n_renglones].astype(str)
    
            resultado = pd.concat([date_col, sensor_col], axis=1)
            resultado = pd.concat([resultado, eth_mac_col], axis=1)
            resultado = pd.concat([resultado, fecha_col], axis=1)
    
            objeto_resultado = resultado.to_records(index=False)    
            lista_resultado=objeto_resultado.tolist()
            dataToSend_resultado = list(map(objeto_remoto, lista_resultado))


            #print("CONTEO B:renglones ANTES",n_renglones)

#        print("duracion SECTOR C---------------------- base de datos abierta", duracion)
        
            status_code = requests.post(url = API_ENDPOINT, json = dataToSend_resultado) #json = 
            print("segunda subida", post_request.status_code)
        
        
            if (post_request.status_code==200):
                tiempo_A=time.time()
                connection = sqlite3.connect('/home/pi/Desktop/hibrido/colector_X.db')
                cursor = connection.cursor()        
                instruccion = "DELETE FROM oeerecords WHERE date < "
                stmd_instr = segunda_tanda_max_date+1
                instruccion = instruccion+str(stmd_instr)
                cursor.execute(instruccion);    
                connection.commit()
                connection.close()
                
                tiempo_B=time.time()
                duracion=tiempo_B-tiempo_A
                #print("BBBB duracion BORRANDO ", duracion)
                
                
                nowe = datetime.now()
                fecha = nowe.strftime("%m/%d/%Y, %H:%M:%S")
                informacion = "".join(str(dataToSend_resultado) for k in dataToSend_resultado)
                pulsos=[(fecha, time.time(),"MAS de 6000", informacion)]
                conn = sqlite3.connect('/home/pi/Desktop/hibrido/colector_Y.db')
                c = conn.cursor()
                c.executemany("INSERT INTO  oeerecords VALUES(?,?,?,?)",pulsos)
                conn.commit()
                conn.close()
                
                
                congelada = congelada.drop(congelada[congelada.date < stmd_instr].index)
                congelada = congelada.reset_index(drop=True)
        
#            print("duracion SECTOR D---------------------- base de datos abierta", duracion)
                n_renglones=len(congelada)

#            print("duracion SECTOR D---------------------- base de datos abierta", duracion)

            
                print("CONTEO B: renglones DESPUES",n_renglones)
                #print("congelada B despues")
                #print(congelada)
                limitadorTemporal_B=time.time()
                limitadorTemporal_Total=limitadorTemporal_B-limitadorTemporal_A


            else:
                print("se perdio conexion en B, no borrar datos")
                nowe = datetime.now()
                fecha = nowe.strftime("%m/%d/%Y, %H:%M:%S")
                informacion = "".join(str(dataToSend_resultado) for k in dataToSend_resultado)
                pulsos=[(fecha, time.time(),"se perdio conexion en B, no borrar datos", informacion)]
                conn = sqlite3.connect('/home/pi/Desktop/hibrido/colector_Y.db')
                c = conn.cursor()
                c.executemany("INSERT INTO  oeerecords VALUES(?,?,?,?)",pulsos)
                conn.commit()
                conn.close()
            ##Cerrar la base de datos
                
  #          except:
            #print("No se subio, no borrar los datos. Tal vez no hay WIFI")

    else:
        print("se perdio conexion en A, no borrar datos")
        nowe = datetime.now()
        fecha = nowe.strftime("%m/%d/%Y, %H:%M:%S")
        informacion = "".join(str(dataToSend_resultado) for k in dataToSend_resultado)
        pulsos=[(fecha, time.time(),"se perdio conexion en A, no borrar datos", informacion)]
        conn = sqlite3.connect('/home/pi/Desktop/hibrido/colector_Y.db')
        c = conn.cursor()
        c.executemany("INSERT INTO  oeerecords VALUES(?,?,?,?)",pulsos)
        conn.commit()
        conn.close()



print(":::::::::::: limitadorTemporal_Total", limitadorTemporal_Total)

