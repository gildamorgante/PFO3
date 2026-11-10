"""
Servidor de tareas - PFO 3

Recibe tareas de los clientes a traves de sockets TCP y las reparte
entre un pool de hilos (workers). Cuando un worker termina, el resultado
se le devuelve al cliente que envio la tarea.

Protocolo: cada mensaje es un JSON en una sola linea terminada en '\n'.
    Pedido:    {"id": 1, "tipo": "sumar", "datos": [1, 2, 3]}
    Respuesta: {"id": 1, "estado": "ok", "resultado": 6, "worker": "worker_0"}
"""

import argparse
import json
import logging
import math
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor

HOST_POR_DEFECTO = "0.0.0.0"
PUERTO_POR_DEFECTO = 5000
WORKERS_POR_DEFECTO = 4

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("servidor")


# ---------------------------------------------------------------------------
# Tareas que sabe resolver el servidor
# ---------------------------------------------------------------------------

def tarea_sumar(datos):
    return sum(datos)


def tarea_multiplicar(datos):
    return math.prod(datos)


def tarea_factorial(n):
    if not isinstance(n, int) or n < 0:
        raise ValueError("el factorial necesita un entero >= 0")
    return math.factorial(n)


def tarea_es_primo(n):
    if not isinstance(n, int):
        raise ValueError("se esperaba un numero entero")
    if n < 2:
        return False
    for i in range(2, math.isqrt(n) + 1):
        if n % i == 0:
            return False
    return True


def tarea_invertir(texto):
    return str(texto)[::-1]


def tarea_contar_palabras(texto):
    return len(str(texto).split())


def tarea_esperar(segundos):
    # Simula un trabajo pesado (por ejemplo, procesar una imagen)
    segundos = float(segundos)
    if segundos < 0 or segundos > 30:
        raise ValueError("los segundos deben estar entre 0 y 30")
    time.sleep(segundos)
    return f"espere {segundos} segundos"


TAREAS = {
    "sumar": tarea_sumar,
    "multiplicar": tarea_multiplicar,
    "factorial": tarea_factorial,
    "es_primo": tarea_es_primo,
    "invertir": tarea_invertir,
    "contar_palabras": tarea_contar_palabras,
    "esperar": tarea_esperar,
}


def ejecutar_tarea(tarea):
    """Lo que corre dentro de cada worker del pool."""
    id_tarea = tarea.get("id")
    tipo = tarea.get("tipo")
    nombre_worker = threading.current_thread().name

    if tipo not in TAREAS:
        return {
            "id": id_tarea,
            "estado": "error",
            "error": f"tipo de tarea desconocido: {tipo}",
            "worker": nombre_worker,
        }

    inicio = time.perf_counter()
    try:
        resultado = TAREAS[tipo](tarea.get("datos"))
        respuesta = {"id": id_tarea, "estado": "ok", "resultado": resultado}
    except Exception as e:
        respuesta = {"id": id_tarea, "estado": "error", "error": str(e)}

    respuesta["worker"] = nombre_worker
    respuesta["duracion_ms"] = round((time.perf_counter() - inicio) * 1000, 2)
    log.info("tarea %s (%s) -> %s", id_tarea, tipo, respuesta["estado"])
    return respuesta


# ---------------------------------------------------------------------------
# Servidor
# ---------------------------------------------------------------------------

class ServidorTareas:
    def __init__(self, host, puerto, cantidad_workers):
        self.host = host
        self.puerto = puerto
        self.pool = ThreadPoolExecutor(
            max_workers=cantidad_workers, thread_name_prefix="worker"
        )
        self.socket_servidor = None

    def iniciar(self):
        self.socket_servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket_servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket_servidor.bind((self.host, self.puerto))
        self.socket_servidor.listen()
        # timeout corto para que Ctrl+C funcione tambien en Windows
        self.socket_servidor.settimeout(1.0)

        log.info("Servidor escuchando en %s:%s con %s workers",
                 self.host, self.puerto, self.pool._max_workers)

        try:
            while True:
                try:
                    conexion, direccion = self.socket_servidor.accept()
                except socket.timeout:
                    continue
                # Un hilo por cliente: solo lee pedidos y los pasa al pool
                hilo = threading.Thread(
                    target=self.atender_cliente,
                    args=(conexion, direccion),
                    daemon=True,
                )
                hilo.start()
        except KeyboardInterrupt:
            log.info("Apagando servidor...")
        finally:
            self.socket_servidor.close()
            self.pool.shutdown(wait=False, cancel_futures=True)

    def atender_cliente(self, conexion, direccion):
        log.info("Cliente conectado: %s:%s", *direccion)
        # Varios workers pueden terminar al mismo tiempo y querer escribir
        # en el mismo socket; el lock evita que se mezclen los mensajes.
        lock_envio = threading.Lock()

        def enviar(mensaje):
            datos = (json.dumps(mensaje) + "\n").encode("utf-8")
            with lock_envio:
                try:
                    conexion.sendall(datos)
                except OSError:
                    pass  # el cliente ya se desconecto

        def al_terminar(futuro):
            enviar(futuro.result())

        try:
            with conexion, conexion.makefile("r", encoding="utf-8") as lector:
                for linea in lector:
                    linea = linea.strip()
                    if not linea:
                        continue
                    try:
                        tarea = json.loads(linea)
                        if not isinstance(tarea, dict):
                            raise ValueError("la tarea debe ser un objeto JSON")
                    except ValueError as e:
                        enviar({"id": None, "estado": "error",
                                "error": f"mensaje invalido: {e}"})
                        continue

                    log.info("Recibida tarea %s de %s:%s",
                             tarea.get("id"), *direccion)
                    futuro = self.pool.submit(ejecutar_tarea, tarea)
                    futuro.add_done_callback(al_terminar)
        except (ConnectionResetError, OSError):
            pass
        log.info("Cliente desconectado: %s:%s", *direccion)


def main():
    parser = argparse.ArgumentParser(description="Servidor de tareas por socket")
    parser.add_argument("--host", default=HOST_POR_DEFECTO)
    parser.add_argument("--puerto", type=int, default=PUERTO_POR_DEFECTO)
    parser.add_argument("--workers", type=int, default=WORKERS_POR_DEFECTO,
                        help="cantidad de hilos en el pool")
    args = parser.parse_args()

    ServidorTareas(args.host, args.puerto, args.workers).iniciar()


if __name__ == "__main__":
    main()
