import sqlite3

conn= sqlite3.connect('/home/pi/Desktop/hibrido/colector_X.db')
c=conn.cursor()

c.execute ('''
           CREATE TABLE IF NOT EXISTS oeerecords
            ([date]STRING PRIMARY KEY,[sensor]TEXT,[eth_mac]TEXT,[fecha]TEXT)
            ''')

print("Base de datos X creada correctamente")





conn= sqlite3.connect('/home/pi/Desktop/hibrido/colector_Y.db')
c=conn.cursor()

c.execute ('''
           CREATE TABLE IF NOT EXISTS oeerecords
            ([date]STRING PRIMARY KEY,[sensor]TEXT,[eth_mac]TEXT,[fecha]TEXT)
            ''')

print("Base de datos Y creada correctamente")



conn= sqlite3.connect('/home/pi/Desktop/hibrido/colector_Z.db')
c=conn.cursor()

c.execute ('''
           CREATE TABLE IF NOT EXISTS oeerecords
            ([date]STRING PRIMARY KEY,[sensor]TEXT,[eth_mac]TEXT,[fecha]TEXT)
            ''')

print("Base de datos Z creada correctamente")


