
import sys
from pyspark import SparkContext
sc = SparkContext()

'''
Adjuntamos a cada anodo el fichero de donde procede, indicado como entrada en la funcion
'''
def get_edges(line,filename):
    edge = line.strip().split(',')
    n1 = edge[0].strip('"')
    n2 = edge[1].strip('"')
    if n1 < n2:
         return ((n1,filename),(n2,filename))
    elif n1 > n2:
         return ((n2,filename),(n1,filename))
    else:
        pass #n1 == n2

'''
Le metemos de entrada rambien el nombre del fichero para que pueda hacer el 
get_edges correctamente
'''
def get_rdd_distict_edges(graph,filename):
    return graph.\
        map(lambda x : get_edges(x,filename)).\
        filter(lambda x: x is not None).\
        distinct() 
        
'''
Nos fijamos ahora en el primer elemento de la tupla para comparar, es decir, el nodo
'''
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


def filtro_triciclos(arista):
    return (len(arista[1])>= 2 and 'exists' in arista[1])


def tricilo(arista):
    triciclo = []
    for elem in arista[1]:
        if elem != 'exists':
            triciclo.append((elem[1],arista[0][0], arista[0][1]))
    return triciclo

'''
Dado un triciclo de la forma ((A,g1),(B,g1),(C,g1)) lo pasamos a (g1,(A,B,C))
'''
def agrupar_triciclos(triciclo):
    return (triciclo[0][1],(triciclo[0][0],triciclo[1][0],triciclo[2][0]))


'''
Dado un grafo en varios archivos de texto, te devuelve los triciclos que contiene cada archivo
por pantalla en una lista, indicando en cada triciclo a que archivo pertenece
'''
def triciclos3(sc,files):
    rdd = sc.parallelize([])
    for file in files: #Ahora hacemos get_rdd_distinct edges en el bucle, para guardar la información del fichero a cada nodo
        file_rdd = sc.textFile(file)
        edges_graph=get_rdd_distict_edges(file_rdd, file)
        rdd = rdd.union(edges_graph) #Los unimos a un mismo rdd y todo igual que en triciclos1 y triciclos2
    nodos_adyacentes = rdd.groupByKey().mapValues(list) 
    lista_conv_adyacentes=nodos_adyacentes.flatMap(conversion_adyacentes) 
    triciclos = lista_conv_adyacentes.groupByKey().mapValues(list).filter(filtro_triciclos).flatMap(tricilo)
    agrupacion_triciclos=triciclos.map(agrupar_triciclos).groupByKey().mapValues(list)
    print(agrupacion_triciclos.collect())
    return agrupacion_triciclos.collect()
    

if __name__ == "__main__":
    archivos = []
    if len(sys.argv) <= 2:
        print(f"Uso: python3 {0} <file>")
    else:
        for i in range(len(sys.argv)):
            if i != 0:
                archivos.append(sys.argv[i])
        triciclos3(sc,archivos)