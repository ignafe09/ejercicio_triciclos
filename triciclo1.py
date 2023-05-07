import sys
from pyspark import SparkContext
sc = SparkContext()

'''
Sea una linea A,B la convertimos en una lista de los elementos sin la coma si son desiguales,
ordenados lexicograficamente, si no None.
'''
def get_edges(line):
    edge = line.strip().replace('"',',').split(',')
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
La pasamos a lista de listas [[A,B],[C,D]] , aparecen solo las distintas y las que no son dos nodos iguales     
'''
def get_rdd_distict_edges(graph):
    return graph.\
        map(get_edges).\
        filter(lambda x: x is not None).\
        distinct() 

''''
nodo_adyacencias es de la forma [A,[B,C]], es decir, nodo y su lista de adyacencias
Esta funcion lo convierte a:
[((A,B),exists),((A,C),exists),((B,C),(pending,A))]
Así facilitaremos la busqueda de aristas que formen triciclos
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
Una vez agrupadas todas las tuplas anteriores en una misma lista, las agrupamos por las aristas
(primer elemento de la tupla), esto esta hecho en el main
, y ahora vemos si alguna arista existe y está pendiente a la vez, que son 
las que nos intersan para formar triciclos, para ver eso le apliacamos esta filtracion.
tupla es de la forma ((A,B),[exists,(pending,C)])
'''
def filtro_triciclos(tupla):
    return ('exists' in tupla[1] and len(tupla[1])>= 2)


'''
Una vez sabemos que se forma un triciclo entre las 3 aristas, formamos el tricilo
tupla es de la forma ((A,B),[exists,(pending,C))])
se devuelve [C,A,B] que es el triciclo que se forma
'''   
def tricilo(tupla):
    triciclo = []
    for elem in tupla[1]:
        if elem != 'exists':
            triciclo.append((elem[1],tupla[0][0], tupla[0][1]))
    return triciclo

'''
Dado un grafo en un archivo de texto, te devuelve los triciclos que contiene
'''
def triciclos1(sc,filename):
    graph = sc.textFile(filename)
    edges_graph = get_rdd_distict_edges(graph)
    nodos_adyacentes = edges_graph.groupByKey().mapValues(list) #Si tenemos [A,B] y [A,C] pasamos a [A,[B,C]], nodo y su lista de adyacencia
    lista_conv_adyacentes=nodos_adyacentes.flatMap(conversion_adyacentes) #Juntamos todas las tuplas nuevas de nodos_adyacentes en una lista
    triciclos = lista_conv_adyacentes.groupByKey().mapValues(list).filter(filtro_triciclos).flatMap(tricilo)
    print(triciclos.collect())
    return triciclos.collect()
    

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Uso: python3 {0} <file>")
    else:
        triciclos1(sc,sys.argv[1])
