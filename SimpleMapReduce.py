#coding=utf-8

'''
Created on 23/04/2018

@author: Fernan
'''

import collections
import itertools
import multiprocessing

class SimpleMapReduce(object):
    
    def __init__(self, map_func, reduce_func, num_workers=None):
        """
        map_func: Funcion para asignar entradas a datos intermedios. Toma como
            argumento un valor de entrada y vuelve a tupla con la clave
            y un valor a ser reducido.
        
        reduce_func: Funcion para reducir la version particionada de los datos intermedios 
          a la salida final. Toma como argumento una clave producida por 
          map_func y una secuencia de los valores asociados con esa clave.
          Establ�zcalo en None para no preocuparse por el resultado
         
        num_workers: La cantidad de trabajadores para crear en el grupo. 
          El valor predeterminado es la cantidad de CPU disponibles en el 
          host actual.
        """
        self.map_func = map_func
        self.reduce_func = reduce_func
        self.pool = multiprocessing.Pool(num_workers)
    
    def partition(self, mapped_values):
        """
            Organiza los valores asignados por su clave.
            Devuelve una secuencia de tuplas sin clasificar con una clave 
            y una secuencia de valores.
        """
        partitioned_data = collections.defaultdict(list)
        for key, value in mapped_values:
            partitioned_data[key].append(value)
        return partitioned_data.items()
    
    def __call__(self, inputs, chunksize=1):
        """
        Procesa las entradas a trav�s del mapa y reduzce las funciones dadas.
        
        inputs
          Un iterable que contiene los datos de entrada para ser procesado.
        
        chunksize=1
          La porci�n de los datos de entrada a mano para cada trabajador. Esta
           se puede usar para ajustar el rendimiento durante la fase de mapeo.
          El número de tareas realizadas cada vez, el valor de la experiencia.
        
        itertools.chain:
           Concatenar objetos iterables para formar un nuevo iterador grande.
            
        """
        map_responses = self.pool.map(self.map_func, inputs, chunksize=chunksize)
        partitioned_data = self.partition(itertools.chain(*map_responses))
        reduced_values = self.pool.map(self.reduce_func, partitioned_data)
        return reduced_values