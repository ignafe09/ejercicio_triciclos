
import sys
from pyspark import SparkContext
sc = SparkContext()


def get_edges(line,filename):
    edge = line.strip().split(',')
    n1 = edge[0]
    n2 = edge[1]
    if n1 < n2:
         return ((n1,filename),(n2,filename))
    elif n1 > n2:
         return ((n2,filename),(n1,filename))
    else:
        pass #n1 == n2
 

def conversion_adyacentes(nodo_adyacencias):
    lista_conv = []
    for i in range(len(nodo_adyacencias[1])):
        lista_conv.append(((nodo_adyacencias[0],nodo_adyacencias[1][i]),'exists'))
        for j in range(i+1,len(nodo_adyacencias[1])):
            if nodo_adyacencias[1][i][0] < nodo_adyacencias[1][j][0]:
                lista_conv.append(((nodo_adyacencias[1][i],nodo_adyacencias[1][j]),('pending',nodo_adyacencias[0])))
            else:
                lista_conv.append(((nodo_adyacencias[1][j],nodo_adyacencias[1][i]),('pending',nodo_adyacencias[0])))
    return lista_conv


def filtro_triciclos(tupla):
    return (len(tupla[1])>= 2 and 'exists' in tupla[1])


def tricilo(tupla):
    triciclo = []
    for elem in tupla[1]:
        if elem != 'exists':
            triciclo.append((elem[1],tupla[0][0], tupla[0][1]))
    return triciclo


'''
Dado un grafo en varios archivos de texto, te devuelve los triciclos que contiene cada archivo en una lista
'''
def triciclos3(sc,files):
    rdd = sc.parallelize([])
    for file in files:
        file_rdd = sc.textFile(file).\
            map(lambda x : get_edges(x,file)).\
            filter(lambda x: x is not None).\
            distinct()
        rdd = rdd.union(file_rdd).distinct() #comprobar si hace falta
    nodos_adyacentes = rdd.groupByKey().mapValues(list) #Si tenemos [A,B] y [A,C] pasamos a [A,[B,C]], nodo y su lista de adyacencia
    lista_conv_adyacentes=nodos_adyacentes.flatMap(conversion_adyacentes) #Juntamos todas las tuplas nuevas de nodos_adyacentes en una lista
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
        triciclos3(sc,archivos)
