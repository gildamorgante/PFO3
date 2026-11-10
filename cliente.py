"""
Cliente de tareas - PFO 3

Se conecta al servidor por socket TCP, envia tareas y muestra los
resultados a medida que van llegando.

Modos de uso:
    python cliente.py                -> envia un lote de tareas de ejemplo
    python cliente.py --interactivo  -> permite escribir tareas a mano
"""

import argparse
import json
import socket
import threading

HOST_POR_DEFECTO = "127.0.0.1"
PUERTO_POR_DEFECTO = 5000

TAREAS_EJEMPLO = [
    {"tipo": "esperar", "datos": 2},
    {"tipo": "sumar", "datos": [10, 20, 30]},
    {"tipo": "multiplicar", "datos": [2, 3, 4]},
    {"tipo": "factorial", "datos": 10},
    {"tipo": "es_primo", "datos": 97},
    {"tipo": "invertir", "datos": "sistemas distribuidos"},
    {"tipo": "contar_palabras", "datos": "hola mundo desde el cliente"},
    {"tipo": "esperar", "datos": 1},
    {"tipo": "tarea_inexistente", "datos": None},
]


class ClienteTareas:
    def __init__(self, host, puerto):
        self.socket = socket.create_connection((host, puerto), timeout=10)
        self.socket.settimeout(None)
        self.lector = self.socket.makefile("r", encoding="utf-8")
        self.siguiente_id = 1

    def enviar(self, tipo, datos):
        tarea = {"id": self.siguiente_id, "tipo": tipo, "datos": datos}
        self.siguiente_id += 1
        self.socket.sendall((json.dumps(tarea) + "\n").encode("utf-8"))
        return tarea["id"]

    def recibir(self):
        """Devuelve la proxima respuesta del servidor, o None si cerro."""
        linea = self.lector.readline()
        if not linea:
            return None
        return json.loads(linea)

    def cerrar(self):
        self.lector.close()
        self.socket.close()


def mostrar(respuesta):
    if respuesta["estado"] == "ok":
        print(f"  <- tarea {respuesta['id']}: {respuesta['resultado']} "
              f"(resuelta por {respuesta['worker']}, {respuesta['duracion_ms']} ms)")
    else:
        print(f"  <- tarea {respuesta['id']}: ERROR - {respuesta['error']}")


def modo_lote(cliente):
    print(f"Enviando {len(TAREAS_EJEMPLO)} tareas al servidor...\n")
    pendientes = set()
    for tarea in TAREAS_EJEMPLO:
        id_tarea = cliente.enviar(tarea["tipo"], tarea["datos"])
        pendientes.add(id_tarea)
        print(f"  -> tarea {id_tarea}: {tarea['tipo']} {tarea['datos']}")

    print("\nResultados (llegan en el orden en que terminan):\n")
    while pendientes:
        respuesta = cliente.recibir()
        if respuesta is None:
            print("El servidor cerro la conexion.")
            break
        mostrar(respuesta)
        pendientes.discard(respuesta["id"])


def modo_interactivo(cliente):
    print("Escribi una tarea con el formato:  tipo datos")
    print("Ejemplos:")
    print("  sumar [1, 2, 3]")
    print("  factorial 5")
    print('  invertir "hola"')
    print("Escribi 'salir' para terminar.\n")

    # Hilo aparte que escucha respuestas, asi podemos seguir escribiendo
    def escuchar():
        while True:
            try:
                respuesta = cliente.recibir()
            except (OSError, ValueError):
                break
            if respuesta is None:
                break
            mostrar(respuesta)

    threading.Thread(target=escuchar, daemon=True).start()

    while True:
        try:
            linea = input("> ").strip()
        except (EOFError, KeyboardInterrupt):
            break
        if not linea:
            continue
        if linea.lower() == "salir":
            break

        tipo, _, texto_datos = linea.partition(" ")
        texto_datos = texto_datos.strip()
        try:
            datos = json.loads(texto_datos) if texto_datos else None
        except json.JSONDecodeError:
            datos = texto_datos  # si no es JSON se manda como texto
        id_tarea = cliente.enviar(tipo, datos)
        print(f"  -> enviada tarea {id_tarea}")


def main():
    parser = argparse.ArgumentParser(description="Cliente de tareas por socket")
    parser.add_argument("--host", default=HOST_POR_DEFECTO)
    parser.add_argument("--puerto", type=int, default=PUERTO_POR_DEFECTO)
    parser.add_argument("--interactivo", action="store_true",
                        help="escribir las tareas a mano")
    args = parser.parse_args()

    try:
        cliente = ClienteTareas(args.host, args.puerto)
    except OSError as e:
        print(f"No se pudo conectar a {args.host}:{args.puerto} ({e})")
        print("Verifica que el servidor este corriendo.")
        return

    print(f"Conectado a {args.host}:{args.puerto}\n")
    try:
        if args.interactivo:
            modo_interactivo(cliente)
        else:
            modo_lote(cliente)
    finally:
        cliente.cerrar()


if __name__ == "__main__":
    main()
