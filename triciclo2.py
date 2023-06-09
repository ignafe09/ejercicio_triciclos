import sys
from pyspark import SparkContext
sc = SparkContext()


def get_edges(line):
    edge = line.strip().split(',')
    n1 = edge[0].strip('"')
    n2 = edge[1].strip('"')
    if n1 < n2:
         return (n1,n2)
    elif n1 > n2:
         return (n2,n1)
    else:
        pass #n1 == n2
 

def get_rdd_distict_edges(graph):
    return graph.\
        map(get_edges).\
        filter(lambda x: x is not None).\
        distinct() 


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


def filtro_triciclos(arista):
    return (len(arista[1])>= 2 and 'exists' in arista[1])


def tricilo(arista):
    triciclo = []
    for elem in arista[1]:
        if elem != 'exists':
            triciclo.append((elem[1],arista[0][0], arista[0][1]))
    return triciclo


'''
Dado un grafo en varios archivos de texto, te devuelve los triciclos que contiene 
en una lista por pantalla
'''
def triciclos2(sc,files):
    rdd=sc.parallelize([])
    for file in files:        #Juntamos toda la informacion de los ficheros en un unico rdd
        file_rdd=sc.textFile(file)
        rdd = rdd.union(file_rdd)
    edges_graph=get_rdd_distict_edges(rdd) #Una vez juntados, igual que en triciclos1
    nodos_adyacentes = edges_graph.groupByKey().mapValues(list) 
    lista_conv_adyacentes=nodos_adyacentes.flatMap(conversion_adyacentes) 
    triciclos = lista_conv_adyacentes.groupByKey().mapValues(list).filter(filtro_triciclos).flatMap(tricilo)
    print(triciclos.collect())
    return triciclos.collect()
    

if __name__ == "__main__":
    archivos = []
    if len(sys.argv) <= 2:
        print(f"Uso: python3 {0} <file>")
    else:
        for i in range(len(sys.argv)):
            if i != 0:
                archivos.append(sys.argv[i])
        triciclos2(sc,archivos)
