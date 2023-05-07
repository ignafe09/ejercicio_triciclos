
import sys
from pyspark import SparkContext
sc = SparkContext()

'''
Los programas están realizados de forma que se reciba un archivo de texto, donde en cada
fila se represente una arista de la forma A,B. Si se introduce de otra forma, habría que 
modificar simplemente la funcion get_edges a tu gusto.
'''
'''
Sea una linea A,B la convertimos en una tupla de los elementos sin la coma si son desiguales,
ordenados lexicograficamente, si no None. (A,B)
'''
def get_edges(line):
    edge = line.strip().split(',')
    n1 = edge[0]
    n2 = edge[1]
    if n1 < n2:
         return (n1,n2)
    elif n1 > n2:
         return (n2,n1)
    else:
        pass #n1 == n2
 
'''
Dado un archivo de un grafo de la forma:
A,B
C,D
La pasamos a lista de tuplas [(A,B),(C,D)] , aparecen solo las distintas y las que no son dos nodos iguales     
'''
def get_rdd_distict_edges(graph):
    return graph.map(get_edges).filter(lambda x: x is not None).distinct() 


''''
nodo_adyacencias es de la forma (A,[B,C]), es decir, nodo y su lista de adyacencias
Esta funcion lo convierte a:
[((A,B),exists),((A,C),exists),((B,C),(pending,A))]
Así facilitaremos la busqueda de aristas que formen triciclos, como se recomienda
en el ejercicio
'''
def conversion_adyacentes(nodo_adyacencias):
    lista_conv = []
    for i in range(len(nodo_adyacencias[1])):
        lista_conv.append(((nodo_adyacencias[0],nodo_adyacencias[1][i]),'exists'))
        for j in range(i+1,len(nodo_adyacencias[1])):
            if nodo_adyacencias[1][i] < nodo_adyacencias[1][j]:
                lista_conv.append(((nodo_adyacencias[1][i],nodo_adyacencias[1][j]),('pending',nodo_adyacencias[0])))
            else:
                lista_conv.append(((nodo_adyacencias[1][j],nodo_adyacencias[1][i]),('pending',nodo_adyacencias[0])))
    return lista_conv


'''
Una vez agrupadas todas las tuplas anteriores de distintos nodos en una misma lista, 
las agrupamos por las aristas (primer elemento de la tupla), esto esta hecho en el main,
y ahora vemos si alguna arista existe y está pendiente a la vez, que son 
las que nos intersan para formar triciclos, para ver eso le apliacamos esta filtracion.
arista es de la forma ((A,B),[exists,(pending,C)])
'''
def filtro_triciclos(arista):
    return ('exists' in arista[1] and len(arista[1])>= 2)


'''
Una vez sabemos que se forma un triciclo entre los 3 nodos, formamos el tricilo
arista es de la forma ((A,B),[exists,(pending,C))])
se devuelve [C,A,B] que es el triciclo que se forma
'''   
def tricilo(arista):
    triciclo = []
    for elem in arista[1]:
        if elem != 'exists':
            triciclo.append((elem[1],arista[0][0], arista[0][1]))
    return triciclo


'''
Dado un grafo en un archivo de texto, te devuelve los triciclos que contiene por pantalla
'''
def triciclos1(sc,filename):
    graph = sc.textFile(filename)
    edges_graph = get_rdd_distict_edges(graph)
    nodos_adyacentes = edges_graph.groupByKey().mapValues(list) #Si tenemos (A,B) y (A,C) agrupamos a (A,[B,C]), nodo y su lista de adyacencia
    lista_conv_adyacentes=nodos_adyacentes.flatMap(conversion_adyacentes) #Juntamos todas las tuplas nuevas de nodos_adyacentes en una lista
    triciclos = lista_conv_adyacentes.groupByKey().mapValues(list).filter(filtro_triciclos).flatMap(tricilo)
    print(triciclos.collect())
    return triciclos.collect()
    

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Uso: python3 {0} <file>")
    else:
        triciclos1(sc,sys.argv[1])