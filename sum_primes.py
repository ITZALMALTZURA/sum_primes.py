#! / usr / bin / python
# Archivo: sum_primes.py
# Autor: VItalii Vanovschi
# Desc: Este programa demuestra cálculos paralelos con el módulo pp
# Calcula la suma de números primos debajo de un entero dado en paralelo
# Software Python paralelo: http : //www.parallelpython.com

import math, sys, time
import pp

def isprime (n):
    "" "Devuelve True si n es primo y False en caso contrario" ""
    si no es isinstance (n, int):
        raise TypeError ( " el argumento pasado a is_prime no es de tipo 'int' " )
    si n < 2 :
        devuelve False
    si n ==2 :
        return True
    max = int (math.ceil (math.sqrt (n)))
    i = 2
    while i <= max:
        if n% i == 0 :
            return False
        i = 1
    return True

def sum_primes (n):
    "" "Calcula la suma de todos los números primos siguientes dado el entero n" ""
    devuelve la suma ([x para x en xrange ( 2 , n) if isprime (x)])

print "" "Uso: python sum_primes.py [ncpus]
    [ ncpus]: el número de trabajadores que se ejecutarán en paralelo,
    si se omite, se establecerá en el número de procesadores en el sistema
"" "

# tupla de todos los servidores Python paralelos para conectarse con
ppservers = ()
#ppservers = (" 10.0.0.1 ",)

if len (sys.argv)> 1 :
    ncpus = int (sys.argv [ 1 ])
    # Crea jobserver con ncpus trabajadores
job_server = pp.Server (ncpus, ppservers = ppservers)
else :
    # Crea jobserver con número de trabajadores detectado automáticamente
job_server = pp.Server (ppservers = ppservers)

print "Comenzando pp con" , job_server.get_ncpus (), "workers"

# Envíe un trabajo de cálculo de sum_primes (100) para su ejecución.
# sum_primes - la función
# (100,) - tupla con argumentos para sum_primes
# (isprime,) - tupla con funciones de las que depende la función sum_primes
# ("matemáticas",) - tupla con nombres de módulo que deben importarse antes de la ejecución de sum_primes
# La ejecución comienza tan pronto como uno de los trabajadores esté disponible
job1 = job_server.submit (sum_primes, ( 100 ,), (isprime,), ( "math" ,))

# Recupera el resultado calculado por job1
# El valor de job1 ( ) es lo mismo que sum_primes (100)
# Si el trabajo aún no se ha terminado, la ejecución esperará aquí hasta que el resultado esté disponible
result = job1 ()

print "La suma de los primos por debajo de 100 es" , resultado

start_time = time.time ()

# Lo siguiente envía 8 trabajos y luego recupera los resultados
input = ( 100000 , 100100 , 100200 , 100300 , 100400 , 100500 , 100600 , 100700 )
jobs = [(input, job_server.submit (sum_primes, ( input,), (isprime,), ( "math" ,))) para entrada en entradas]
para entrada, trabajo en trabajos:
    imprimir "Suma de primos debajo" , entrada, "es" , trabajo ()

imprimir "Tiempo transcurrido : ", time.time () - start_time, "s"
job_server.print_stats ()

# Parallel Python Software: http://www.parallelpython.com
