#coding=utf-8

'''
Created on 23/04/2018

@author: dazuluag@unal.edu.co
'''

import re
import string
import operator
import glob

from datetime import datetime
from dateutil.relativedelta import relativedelta

from SimpleMapReduce import SimpleMapReduce

# Funcion que permite cargar el archivo con los datos de los articulos de Wikipedia
def cargar_archivo(ruta):
    #archivo = open(ruta, 'r', encoding="utf8").read()
    
    # Listado de articulos cargados desde el archivo Wikipedia
    #articulos = archivo.split(',\n')
    
    # Listado de lenguajes de programacion para la busqueda
    lenguajes = list(set(["JavaScript", "Java", "PHP", "Python", "C#", 
                          "C++", "Ruby", "CSS", "Objective-C", "Perl", 
                          "Scala", "Haskell", "MATLAB", "Clojure", "Groovy"]))
    
    # Tabla de asignacion y reemplazo de la puntuacion con espacios
    # excepto los caracteres + - #
    puntuacion = string.punctuation.replace('+', '').replace('-', '').replace('#', '')
    TR = str.maketrans(puntuacion, ' ' * len(puntuacion))
    
    # Declaro listado de registros con tuplas (lenguaje, ocurrencias)
    ocurrencias = []
    
    # Recorro listado de articulos buscando lenguajes de programacion
    with open(ruta, 'r', encoding="utf8") as archivo:
        # Listado de articulos cargados desde el archivo Wikipedia
        articulos = archivo.read().split(',\n')
        print("Cantidad de articulos: {}\n".format(len(articulos)))
        for articulo in articulos:
            articulo = articulo.translate(TR)
            articulo = articulo.lower()
            for lenguaje in lenguajes:
                lenguaje = lenguaje.lower()
                texto_re = r'\b'+re.escape(lenguaje)+r'\b'
                if len(re.findall(texto_re, articulo)) > 0:
                    # Cuando encuentro el lenguaje en un articulo, genero un tupla
                    # con el lenguaje y el valor de 1 (para sumar estos despues)
                    ocurrencias.append((lenguaje, 1))
    return(ocurrencias)

# Funcion que permite contar la cantidad de veces que aparece un lenguaje en un articulo
def contar_palabras(tupla):
    lenguaje, ocurrencias = tupla
    return(lenguaje, sum(ocurrencias))

# Funcion para calcular el tiempo de ejecucion
def diff(t_a, t_b):
    t_diff = relativedelta(t_b, t_a)  # later/end time comes first!
    return('{h}h {m}m {s}s'.format(h=t_diff.hours, m=t_diff.minutes, s=t_diff.seconds))
    
if __name__ == '__main__':
    ruta_archivos = glob.glob('resources/wikipedia_1000.dat')
    tiempo_ini = datetime.now()
    mapper = SimpleMapReduce(cargar_archivo, contar_palabras, 15)
    cuenta_leng = mapper(ruta_archivos)
    cuenta_leng.sort(key=operator.itemgetter(1))
    cuenta_leng.reverse()
    maximo_leng = max(len(leng) for leng, cant in cuenta_leng)
    for leng, cant in cuenta_leng:
        print( '%-*s: %5s' % (maximo_leng+1, leng, cant))
    tiempo_fin = datetime.now()
    print("\nTiempo de ejecucion: {}".format(diff(tiempo_ini, tiempo_fin)))
    
    
    
    

