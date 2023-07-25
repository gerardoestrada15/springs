#! /usr/bin/python3
#@lxterminal -e python3 /home/pi/Desktop/medicion.py
#import numpy as np
#import pandas as pd
import automationhat
from datetime import date
from datetime import datetime
import time
import ipaddress
import requests
import json
import os
import threading
import subprocess
import sqlite3
import random
import threading
import logging
from concurrent.futures import ThreadPoolExecutor
logging.basicConfig(level=logging.DEBUG, format='%(threadName)s: %(message)s')



proc = subprocess.Popen(["cat", "/sys/class/net/eth0/address"], stdout=subprocess.PIPE, shell=False)
(eth_mac, err) =proc.communicate()

print("direccion MAC ",eth_mac)



three = False
two = False
one = False

deltaT=0
currT=0

prevT=time.time()
bandera_A=1
bandera_B=1
bandera_C=1

parA=1
parB=1
parC=1

diferenciaDeTiempoA=0
tiempo1A=0
tiempo2A=0
diferenciaDeTiempoB=0
tiempo1B=0
tiempo2B=0
diferenciaDeTiempoC=0
tiempo1C=0
tiempo2C=0

carrouselBuffer="X"

tresholdA=0
tresholdB=0
tresholdC=0

sensorA=1
sensorB=2
sensorC=3

contadorSensorA=0
contadorSensorB=0
contadorSensorC=0

contadorSensorCortoA=0
contadorSensorCortoB=0
contadorSensorCortoC=0

parW=1
tiempo1W=0
diferenciaDeTiempoW=0
tiempo2W=0

parQ=1
tiempo1Q=0
diferenciaDeTiempoQ=0
tiempo2Q=0

inicializa=True
siempre=False
buenaActivada=True

ContadorBuenas=0

scrap_final=0

aritmetica=0

diferencia=0

#@lxterminal -e python3 /home/pi/Desktop/medicion.py


def escribe(eth_mac,boton,dateTIME,carrouselBuffer):
    sensor= boton
    #t1=time.time()
    #tiempoA=time.time()
    nowe =datetime.now()
    date_time=nowe.strftime("%m/%d/%Y, %H:%M:%S")
    #eth_mac="dc:a6:32:12:61:e6"  #oeetest
    eth_mac="e4:5f:01:e3:f4:e6" #springs

    fecha= datetime.today().strftime('%Y-%m-%d')
    hora=date_time = nowe.strftime("%m/%d/%Y, %H:%M:%S")		
    date=time.time()*1000000
    #epoch=Date.now()
    date=str(date)
    date=date+date

    #    print(date[0:13])
    pulsos=[
    (date[0:13],sensor,eth_mac,date_time)
    ]

    if carrouselBuffer=="Z":
        conn = sqlite3.connect('/home/pi/Desktop/hibrido/colector_Z.db')

    elif carrouselBuffer=="X":
        conn = sqlite3.connect('/home/pi/Desktop/hibrido/colector_X.db')

    elif carrouselBuffer=="Y":
        conn = sqlite3.connect('/home/pi/Desktop/hibrido/colector_Y.db')

    elif carrouselBuffer=="A":
        conn = sqlite3.connect('/home/pi/Desktop/hibrido/colector_A.db')

    c = conn.cursor()
    c.executemany("INSERT INTO  oeerecords VALUES(?,?,?,?)",pulsos)
    conn.commit()
    conn.close()
    
    conn = sqlite3.connect('/home/pi/Desktop/hibrido/colector_Z.db')
    c = conn.cursor()
    c.executemany("INSERT INTO  oeerecords VALUES(?,?,?,?)",pulsos)
    conn.commit()
    conn.close()
    
    #tiempoB=time.time()
    #duracion=tiempoB-tiempoA
    #print("duracion escritura",duracion)

#-----------------------------------------------------------------------------------------

def subir_datos_a_la_nubeX():
    #pass
    print("Subio a la nube en X")
    subprocess.run([ "python","/home/pi/Desktop/hibrido/readandupload3_X.py"])
def subir_datos_a_la_nubeY():
    #pass
    print("Subio a la nube en Y")
    subprocess.run([ "python","/home/pi/Desktop/hibrido/readandupload3_Y.py"])
def subir_datos_a_la_nubeZ():
    #pass
    print("Subio a la nube en Z")
    subprocess.run([ "python","/home/pi/Desktop/hibrido/readandupload3_Z.py"])

def treshold(diferenciaDeTiempo,treshold,sensor,contadorSensorCorto,contadorSensor,carrouselBuffer):
        nowe = datetime.now()
        date_time = nowe.strftime("%m/%d/%Y, %H:%M:%S")
        if diferenciaDeTiempo<treshold:
            contadorSensorCorto=contadorSensorCorto+1

            print(f"{date_time} NO CONTADO Tiempo SENSOR {sensor} pequenno {diferenciaDeTiempo}, anomalia {contadorSensorCorto}")
        else:
            contadorSensor=contadorSensor+1
            print("---sensor", sensor, "diferenciaDeTiempo", diferenciaDeTiempo)
            if not (sensor==3):
                escribe(eth_mac,sensor,date_time,carrouselBuffer)
            else:
                #print("sensor 3:",sensor)
                pass
 #           if contadorSensor%1==0:
            if sensor==2:
                #escribe(eth_mac,sensor,date_time,carrouselBuffer)
#                print(f"{sensor}- {date_time} normal {diferenciaDeTiempo}, numero {contadorSensor}")
                print(f"{date_time} Tiempo SENSOR {sensor} grande {diferenciaDeTiempo}, evento {contadorSensor}")

        return contadorSensorCorto, contadorSensor



nowe = datetime.now()
date_time = nowe.strftime("%m/%d/%Y, %H:%M:%S.%f")
print("********************Iniciando programa...",date_time )

executor=ThreadPoolExecutor(max_workers=1)

#piezas=1
#tiempoPiezas1=time.time()
#totalPiezas=50+1

time.sleep(1)

while True:#piezas<totalPiezas:#True:
    currT=time.time()
    deltaT=currT-prevT

    if (deltaT > 60):# or (piezas==totalPiezas-1):

        prevT=currT
        nowe = datetime.now()
        date_time = nowe.strftime("%m/%d/%Y, %H:%M:%S")

        tiempoA=time.time()

        if carrouselBuffer=="X":
            executor.submit(subir_datos_a_la_nubeX)
            tiempoB=time.time()
            deltaT=tiempoB-tiempoA
 #           print(date_time,'Subiendo a la nube X', deltaT)
            carrouselBuffer="X"

        elif carrouselBuffer=="Y":
            executor.submit(subir_datos_a_la_nubeZ)
            tiempoB=time.time()
            deltaT=tiempoB-tiempoA
   #         print(date_time,'Subiendo a la nube Z', deltaT)
            carrouselBuffer="X"

        elif carrouselBuffer=="Z":
            executor.submit(subir_datos_a_la_nubeX)
            tiempoB=time.time()
            deltaT=tiempoB-tiempoA
  #          print(date_time,'Subiendo a la nube X', deltaT)
            carrouselBuffer="Y"

        elif carrouselBuffer=="A":
            #subir_datos_a_la_nubeA()
            executor.submit(subir_datos_a_la_nubeA)
            tiempoB=time.time()
            deltaT=tiempoB-tiempoA
  #          print(date_time,'Subiendo a la nube A', deltaT)
            carrouselBuffer="A"
        #treshold=0.00001
        #if deltaT>treshold:
        #    pass
            #print("treshold",treshold,'Subiendo a la nube',deltaT)


    three= automationhat.input.three.is_on()
    if (three and bandera_C==1):
        bandera_C=0

        if parC==1:
            tiempo1C=time.time()
            diferenciaDeTiempoC=tiempo1C-tiempo2C
   #         print(date_time, "contador", contador_defectos, ", tiempo entre defectos",diferenciaDeTiempoX)
            parC=2
        elif parC==2:
            tiempo2C=time.time()
            parC=1
            diferenciaDeTiempoC=tiempo2C-tiempo1C

        CRONO_C1=time.time()
  #          print(date_time, "CONTADOR", contador_defectos, ", TIEMPO ENTRE DEFECTOS",diferenciaDeTiempoX)

#        contadorSensorCortoC,contadorSensorC=treshold(diferenciaDeTiempoC,tresholdC,3,contadorSensorCortoC,contadorSensorC)
    elif (not three and bandera_C==0):
        bandera_C=1
        CRONO_C2=time.time()
        contadorSensorCortoC,contadorSensorC=treshold(CRONO_C2-CRONO_C1,tresholdC,3,contadorSensorCortoC,contadorSensorC,carrouselBuffer)
        nowe = datetime.now()
        date_time = nowe.strftime("%H:%M:%S.%f")
        print("contadorSensorC",contadorSensorC, "hora", date_time)
 #       print("contadorSensorC",contadorSensorC)
        if inicializa:
            inicializa=False


    two= automationhat.input.two.is_on()
    if (two and bandera_B==1):#and siempre:
        bandera_B=0

        if parB==1:
            tiempo1B=time.time()
            diferenciaDeTiempoB=tiempo1B-tiempo2B
   #         print(date_time, "contador", contador_defectos, ", tiempo entre defectos",diferenciaDeTiempoX)
            parB=2
        elif parB==2:
            tiempo2B=time.time()
            parB=1
            diferenciaDeTiempoB=tiempo2B-tiempo1B
  #          print(date_time, "CONTADOR", contador_defectos, ", TIEMPO ENTRE DEFECTOS",diferenciaDeTiempoX)

 #       contadorSensorCortoB,contadorSensorB=treshold(diferenciaDeTiempoB,tresholdB,2,contadorSensorCortoB,contadorSensorB)
        CRONO_B1=time.time()
    elif (not two and bandera_B==0):# and siempre:
        bandera_B=1
        CRONO_B2=time.time()
        scrap_final=scrap_final+1
        print(date_time,"++++++++++++++++++++scrap al final",scrap_final)
        if ((contadorSensorC-contadorSensorA)<=0) and aritmetica==0:
            nowe = datetime.now()
            date_time = nowe.strftime("%m/%d/%Y, %H:%M:%S.%f")
            #scrap_final=scrap_final+1
            #print(date_time,"++++++++++++++++++++scrap al final",scrap_final)
            contadorSensorCortoB,contadorSensorB=treshold(CRONO_B2-CRONO_B1,tresholdB,2,contadorSensorCortoB,contadorSensorB,carrouselBuffer)
            contadorSensorC=0
            contadorSensorA=0

        if parQ==1:
            parQ=2
            tiempo1Q=time.time()
            diferenciaDeTiempoQ=tiempo1Q-tiempo2Q
        elif parQ==2:
            tiempo2Q=time.time()
            parQ=1
            diferenciaDeTiempoQ=tiempo2Q-tiempo1Q
  #      print("tiempo inter pulso 2",diferenciaDeTiempoQ)




    one= automationhat.input.one.is_on()
    if (one and bandera_A==1): #( not oneprev)
        nowe = datetime.now()
        date_time = nowe.strftime("%m/%d/%Y, %H:%M:%S")
        bandera_A=0

        if parA==1:
            parA=2
            tiempo1A=time.time()
            diferenciaDeTiempoA=tiempo1A-tiempo2A
        elif parA==2:
            tiempo2A=time.time()
            parA=1
            diferenciaDeTiempoA=tiempo2A-tiempo1A
        CRONO_A1=time.time()
#        contadorSensorCortoA,contadorSensorA=treshold(diferenciaDeTiempoA,tresholdA,1,contadorSensorCortoA,contadorSensorA)
    elif (not one and bandera_A==0):
        bandera_A=1
        CRONO_A2=time.time()
        contadorSensorCortoA,contadorSensorA=treshold(CRONO_A2-CRONO_A1,tresholdA,1,contadorSensorCortoA,contadorSensorA,carrouselBuffer)
        ContadorBuenas=ContadorBuenas+1
#        print("Contador Buenas", ContadorBuenas)
        buenaActivada=True
        aritmetica=0

        if parW==1:
            parW=2
            tiempo1W=time.time()
            diferenciaDeTiempoW=tiempo1W-tiempo2W
        elif parW==2:
            tiempo2W=time.time()
            parW=1
            diferenciaDeTiempoW=tiempo2W-tiempo1W
        nowe = datetime.now()
        date_time = nowe.strftime("%H:%M:%S.%f")
        print("contadorSensorA",contadorSensorA, "hora", date_time)

    diferencia=contadorSensorC-contadorSensorA

    if diferencia<=-1:
        contadorSensorC=0#0
        contadorSensorA=0
        print("el sensor de ensamble A se activo 2 veces, o se perdio un pulso de C")

    if diferencia>=2:
        buenaActivada=False
        contadorSensorC=1
        contadorSensorA=0
        aritmetica=1

        contadorSensorCortoB,contadorSensorB=treshold(2.02020202,tresholdB,2,contadorSensorCortoB,contadorSensorB,carrouselBuffer)
        print("defecto por aritmetica")

        #contadorSensorC=0
        #contadorSensorA=0
        inicializa=True


        #print("tiempo inter pulso",diferenciaDeTiempoW)
#tiempoPiezas2=time.time()
#duracionPiezas=tiempoPiezas2-tiempoPiezas1
#frecuencia=(totalPiezas-1)/duracionPiezas
#print("Frecuencia de sampleo",frecuencia)
