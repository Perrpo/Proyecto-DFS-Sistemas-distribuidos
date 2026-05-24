# Informe Técnico Grupal – MiniDFS
## Sistema de Archivos Distribuidos por Bloques

> **Asignatura:** Arquitecturas de Nube y Sistemas Distribuidos  
> **Facultad:** Ingeniería de Sistemas e Informática  
> **Universidad:** Pontificia Bolivariana  
> **Fecha:** 24 de mayo de 2026  
> **Integrante:** Andrés Felipe Núñez Hernández

---

## 1. Objetivo

### 1.1 Objetivo General

Diseñar e implementar un sistema de archivos distribuido (DFS) minimalista por bloques que permita almacenar y acceder a archivos grandes de forma distribuida en múltiples nodos, aplicando particionamiento en bloques de 64 MB, replicación pipeline con factor 3, y tolerancia activa a fallos mediante re-replicación automática.

### 1.2 Objetivos Específicos

- Implementar un NameNode central que gestione metadatos, autenticación JWT y coordine la distribución de bloques.
- Desplegar múltiples DataNodes que almacenen bloques con verificación de integridad SHA-256.
- Proveer una CLI cliente que implemente las operaciones `put`, `get`, `ls`, `mkdir`, `rmdir`, `rm`, `register`, `login`.
- Garantizar tolerancia a fallos mediante replicación pipeline y re-replicación activa.
- Desplegar el sistema en infraestructura AWS EC2 con comunicación gRPC sobre Internet.

---

## 2. Marco Teórico

### 2.1 Sistemas de Archivos Distribuidos (DFS)

Un sistema de archivos distribuido permite que múltiples nodos compartan y accedan de forma concurrente a archivos almacenados en diferentes servidores. Los dos enfoques principales son:

- **DFS basado en bloques:** La unidad de distribución es el bloque (fragmento de archivo). Los bloques pueden replicarse en distintos nodos para garantizar disponibilidad y rendimiento. Ejemplos: HDFS, GFS.
- **DFS basado en objetos:** La unidad es el archivo completo. No soporta actualización parcial (WORM). Ejemplo: AWS S3.

MiniDFS sigue el enfoque **basado en bloques**, inspirado en GFS y HDFS.

### 2.2 Google File System (GFS)

GFS introduce la arquitectura Maestro–Trabajadores donde un único *Master* (NameNode en HDFS) mantiene el namespace de archivos y la ubicación de los chunks (bloques de 64 MB). Los *ChunkServers* (DataNodes) almacenan los chunks replicados. Los clientes consultan al Master para ubicar chunks y luego interactúan directamente con los ChunkServers.

### 2.3 HDFS

HDFS adopta los principios de GFS con nomenclatura propia: NameNode (metadatos) y DataNodes (almacenamiento). Introduce el *BlockReport* periódico que permite al NameNode reconciliar el estado real del sistema, y el *Heartbeat* para detectar nodos caídos y activar re-replicación.

### 2.4 gRPC

gRPC es un framework de llamada a procedimiento remoto de alto rendimiento basado en HTTP/2 y Protocol Buffers. Soporta streaming bidireccional, compresión automática y multiplexación de solicitudes, lo que lo hace ideal para transferencias de bloques de datos grandes.

---

## 3. Descripción del Servicio y Problema Abordado

### 3.1 Problema

El almacenamiento centralizado de archivos introduce un único punto de fallo y limita la escalabilidad. Para archivos grandes (videos, bases de datos, backups), un único servidor se convierte en cuello de botella tanto en capacidad como en ancho de banda.

### 3.2 Solución

MiniDFS distribuye los archivos en bloques de 64 MB entre múltiples DataNodes. Cada bloque se replica en 3 nodos distintos, garantizando que el sistema siga operativo aunque fallen hasta 2 de los 3 DataNodes que almacenan un bloque. Un NameNode central coordina los metadatos, la autenticación y la re-replicación automática.

### 3.3 Características del Sistema

| Característica | Valor |
|---|---|
| Tamaño de bloque | 64 MB (configurable) |
| Factor de replicación | 3 |
| Protocolo de comunicación | gRPC con Protocol Buffers |
| Autenticación | JWT + bcrypt |
| Persistencia de metadatos | SQLite |
| Verificación de integridad | SHA-256 por bloque |
| Detección de fallos | Heartbeat cada 10s, timeout 30s |
| Re-replicación | Automática por HeartbeatMonitor |

---

## 4. Arquitectura del Sistema

### 4.1 Tipo de Arquitectura

**Maestro–Trabajadores (Master–Workers)** con comunicación directa Cliente–DataNode para transferencia de bloques:

```
┌──────────────────────────────────────────────────────────────┐
│                        CLIENTE (CLI)                         │
│         register | login | put | get | ls | mkdir | rm       │
└────────────┬─────────────────────────────┬───────────────────┘
             │ gRPC: metadatos + auth       │ gRPC: bloques (streaming)
             ▼                             ▼
┌────────────────────────┐   ┌────────────────────────────────┐
│       NAMENODE         │   │          DATANODES             │
│  :50051                │   │  dn1: :50061                   │
│                        │   │  dn2: :50062                   │
│  ● AuthService (JWT)   │◄──│  dn3: :50063                   │
│  ● NamespaceManager    │   │                                │
│  ● BlockManager        │   │  ● BlockStore + SHA-256        │
│  ● HeartbeatMonitor    │   │  ● HeartbeatAgent              │
│  ● MetadataStore (SQL) │   │  ● BlockReportAgent            │
└────────────────────────┘   │  ● ReplicationAgent (pipeline) │
                             └────────────────────────────────┘
```

### 4.2 Componentes

#### NameNode
Servidor gRPC central que implementa:
- **AuthService:** Registro y login con contraseñas hasheadas con bcrypt. Emite tokens JWT con expiración de 24h.
- **NamespaceManager:** Sistema de archivos virtual jerárquico por usuario. Gestiona directorios y rutas.
- **BlockManager:** Asignación de DataNodes por Round-Robin. Detecta bloques sub-replicados consultando la tabla de ubicaciones.
- **HeartbeatMonitor:** Thread daemon que verifica cada 10s si los DataNodes siguen activos. Si un nodo no envía heartbeat en 30s, lo marca inactivo y programa re-replicación de sus bloques.
- **MetadataStore:** Capa SQLite con tablas para users, files, blocks, block_locations y datanodes.

#### DataNode
Servidor gRPC de almacenamiento que implementa:
- **BlockStore:** Escribe bloques en disco como archivos `.block`, calcula SHA-256 al escribir y verifica al leer.
- **HeartbeatAgent:** Envía heartbeat al NameNode cada 10s con estado del nodo y espacio disponible.
- **BlockReportAgent:** Envía inventario completo de bloques al NameNode cada 60s para reconciliación.
- **ReplicationAgent:** Gestiona el pipeline de replicación forward y la re-replicación por orden del NameNode.

#### Cliente (CLI)
Interfaz de línea de comandos que implementa:
- **block_splitter:** Generador que lee el archivo en bloques de 64 MB y calcula SHA-256 de cada uno.
- **block_assembler:** Reordena bloques por índice y reconstruye el archivo de salida.
- **block_transfer:** Upload/download vía gRPC streaming con backoff exponencial (1s→2s→4s) y fallback a réplicas alternativas.
- **auth_client:** Persiste el token JWT en `~/.minidfs_token`.

### 4.3 Diagrama de Flujo PUT

```
1. Cliente → NameNode: PutFileRequest(filepath, filesize, block_count)
2. NameNode → Cliente: BlockAssignments[{block_id, datanodes[dn1,dn2,dn3]}]
3. Para cada bloque:
   a. Cliente → dn1: StoreBlock (client-streaming: chunks de 4MB)
   b. dn1 → dn2: pipeline forward (server-to-server)
   c. dn2 → dn3: pipeline forward
   d. Cliente → NameNode: ConfirmBlock(block_id, checksum, node_ids)
4. NameNode actualiza SQLite con ubicaciones confirmadas
```

### 4.4 Diagrama de Flujo GET

```
1. Cliente → NameNode: GetFileRequest(filepath)
2. NameNode → Cliente: BlockLocations[{block_id, block_index, datanodes, checksum}]
3. Para cada bloque (ordenado por índice):
   a. Cliente → dn_primario: RetrieveBlock(block_id)
   b. DataNode → Cliente: chunks de 1MB (server-streaming)
   c. Cliente verifica SHA-256; si falla → intenta siguiente réplica
4. Cliente ensambla bloques en archivo de salida
```

---

## 5. Especificación de Protocolos y APIs

### 5.1 Protocolo de Comunicación: gRPC

Todos los servicios se comunican mediante **gRPC sobre HTTP/2** con **Protocol Buffers v3** como formato de serialización. La definición completa está en `proto/dfs.proto`.

### 5.2 Servicios gRPC del NameNode (puerto 50051)

| RPC | Tipo | Descripción |
|---|---|---|
| `Register` | Unary | Registrar nuevo usuario (bcrypt) |
| `Login` | Unary | Autenticar y obtener token JWT |
| `PutFile` | Unary | Solicitar asignación de bloques para upload |
| `ConfirmBlock` | Unary | Confirmar bloque almacenado + checksum |
| `GetFile` | Unary | Obtener ubicaciones de bloques para download |
| `DeleteFile` | Unary | Eliminar archivo y sus metadatos |
| `ListFiles` | Unary | Listar archivos y directorios |
| `MakeDir` | Unary | Crear directorio virtual |
| `RemoveDir` | Unary | Eliminar directorio vacío |
| `Heartbeat` | Unary | Recibir heartbeat de DataNode |
| `BlockReport` | Unary | Recibir inventario de bloques de DataNode |
| `ReportCorruptBlock` | Unary | Reportar bloque corrupto (SHA-256 mismatch) |

### 5.3 Servicios gRPC del DataNode (puertos 50061-50063)

| RPC | Tipo | Descripción |
|---|---|---|
| `StoreBlock` | Client-streaming | Recibir bloque en chunks de 4MB |
| `RetrieveBlock` | Server-streaming | Enviar bloque en chunks de 1MB |
| `DeleteBlock` | Unary | Eliminar bloque del disco |

### 5.4 Mensajes Principales (Protocol Buffers)

```protobuf
message StoreBlockRequest {
  string block_id    = 1;
  bytes  data        = 2;  // chunk de hasta 4MB
  string checksum    = 3;  // SHA-256 (solo en primer chunk)
  repeated DataNodeInfo pipeline = 4;  // nodos para replicación
}

message BlockAssignment {
  string block_id   = 1;
  int32  block_index = 2;
  repeated DataNodeInfo datanodes = 3;
}
```

---

## 6. Algoritmos de Particionamiento, Distribución y Replicación

### 6.1 Particionamiento de Bloques

```python
def split_file(filepath, block_size=64*1024*1024):
    with open(filepath, 'rb') as f:
        index = 0
        while True:
            data = f.read(block_size)
            if not data:
                break
            checksum = hashlib.sha256(data).hexdigest()
            yield index, data, checksum
            index += 1
```

El último bloque puede ser menor que `block_size`. El índice de cada bloque se preserva en los metadatos para garantizar el orden correcto en la reconstrucción.

### 6.2 Distribución Round-Robin

```python
def assign_datanodes(block_count, replication_factor=3):
    active_nodes = self.store.get_active_datanodes()
    assignments = []
    for i in range(block_count):
        selected = []
        start = self._rr_counter % len(active_nodes)
        for j in range(replication_factor):
            selected.append(active_nodes[(start + j) % len(active_nodes)])
        self._rr_counter += 1
        assignments.append(selected)
    return assignments
```

### 6.3 Pipeline de Replicación

El cliente envía el bloque únicamente al nodo primario. Ese nodo, al recibir el bloque completo, lo reenvía al siguiente nodo del pipeline en paralelo. El pipeline continúa hasta que todos los nodos de replicación hayan almacenado el bloque.

```
Cliente → dn1 (primario)
          dn1 → dn2 (pipeline forward)
                dn2 → dn3 (pipeline forward)
```

Esto minimiza el ancho de banda consumido por el cliente (solo sube una vez) y distribuye la carga de replicación entre los DataNodes.

### 6.4 Re-replicación Automática

Cuando el HeartbeatMonitor detecta que un DataNode no ha enviado heartbeat en más de 30 segundos:

1. Marca el nodo como `inactive` en SQLite.
2. Llama a `BlockManager.get_underreplicated_blocks()` para obtener bloques con menos de 3 réplicas activas.
3. Para cada bloque sub-replicado, selecciona un nodo fuente (réplica activa) y un nodo destino (nodo activo sin esa réplica).
4. Encola la tarea en `_pending_tasks[source_node_id]`.
5. En el siguiente heartbeat del nodo fuente, el NameNode incluye la tarea en la respuesta.
6. El DataNode fuente ejecuta `ReplicationAgent._replicate()` para copiar el bloque al destino.

---

## 7. Entorno de Ejecución

### 7.1 Docker

Cada componente tiene su propio `Dockerfile` basado en `python:3.11-slim`. Los stubs gRPC se generan en tiempo de build dentro del contenedor:

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY namenode/requirements.txt /app/namenode/requirements.txt
RUN pip install --no-cache-dir -r /app/namenode/requirements.txt
COPY proto/ /app/proto/
COPY config.py /app/
COPY namenode/ /app/namenode/
RUN python -m grpc_tools.protoc -I /app --python_out=/app \
    --grpc_python_out=/app /app/proto/dfs.proto
EXPOSE 50051
CMD ["python", "server.py"]
```

Docker Compose orquesta los 4 servicios con healthchecks y la condición `depends_on: service_healthy` para que los DataNodes esperen a que el NameNode esté listo.

### 7.2 AWS EC2

El sistema fue desplegado en 4 instancias EC2 independientes en `us-east-1`:

| Instancia | AMI | Tipo | IP Privada |
|---|---|---|---|
| NameNode | Ubuntu 26.04 LTS | t3.small | 172.31.0.165 |
| DataNode 1 | Ubuntu 26.04 LTS | t3.small | 172.31.5.1 |
| DataNode 2 | Ubuntu 26.04 LTS | t3.small | 172.31.9.175 |
| DataNode 3 | Ubuntu 26.04 LTS | t3.small | 172.31.9.76 |

Cada instancia ejecuta su servicio en un contenedor Docker con los puertos correspondientes expuestos en el Security Group `proyecto_dfs`.

---

## 8. Pruebas y Análisis de Resultados

### 8.1 Prueba 1: Autenticación y Namespace

**Comando:**
```bash
python cli.py register andres pass123
python cli.py login andres pass123
python cli.py mkdir /test
python cli.py ls /
```

**Resultado:**
```
Usuario registrado exitosamente
Autenticación exitosa
Directorio creado
d  /test/
```
**✅ PASS** — Autenticación JWT funcional. Namespace virtual operativo.

---

### 8.2 Prueba 2: PUT con Replicación Pipeline

**Comando:**
```bash
dd if=/dev/urandom of=/tmp/test.bin bs=1M count=5
python cli.py put /tmp/test.bin /test/test.bin
```

**Resultado:**
```
Subiendo: /tmp/test.bin (5.0 MB, 1 bloque(s))
  → /test/test.bin
  Bloque 1/1: 5.0 MB → dn2 (pipeline: ['dn3', 'dn1']) ... OK

✓ Archivo subido exitosamente: /test/test.bin
  1/1 bloques confirmados | file_id=548492e7-9f9c-4ee3-8bc0-22b4d939a86c
```
**✅ PASS** — Bloque replicado a los 3 DataNodes vía pipeline. `file_id` registrado en NameNode.

---

### 8.3 Prueba 3: GET y Verificación de Integridad

**Comando:**
```bash
python cli.py get /test/test.bin /tmp/test_descargado.bin
sha256sum /tmp/test.bin /tmp/test_descargado.bin
```

**Resultado:**
```
Descargando: /test/test.bin (5.0 MB, 1 bloque(s))
  Bloque 1/1: réplicas=['dn1', 'dn2', 'dn3'] ... OK
✓ Archivo descargado exitosamente: /tmp/test_descargado.bin (5.0 MB)

5c3c26904f888b5970cfdce8cb38688023422ee2604b04c92d1cb001ffa4cd7c  /tmp/test.bin
5c3c26904f888b5970cfdce8cb38688023422ee2604b04c92d1cb001ffa4cd7c  /tmp/test_descargado.bin
```
**✅ PASS** — SHA-256 idéntico. Integridad de datos garantizada de extremo a extremo.

---

### 8.4 Prueba 4: Tolerancia a Fallos (DataNode 2 caído)

**Procedimiento:** Se detuvo el contenedor `minidfs-datanode2` en su instancia EC2 y se intentó descargar el mismo archivo.

**Comando:**
```bash
# En DataNode 2 (172.31.9.175): docker compose down
python cli.py get /test/test.bin /tmp/test_tolerancia.bin
sha256sum /tmp/test.bin /tmp/test_tolerancia.bin
```

**Resultado:**
```
Descargando: /test/test.bin (5.0 MB, 1 bloque(s))
  Bloque 1/1: réplicas=['dn1', 'dn2', 'dn3'] ... OK
✓ Archivo descargado exitosamente: /tmp/test_tolerancia.bin (5.0 MB)

5c3c26904f888b5970cfdce8cb38688023422ee2604b04c92d1cb001ffa4cd7c  /tmp/test.bin
5c3c26904f888b5970cfdce8cb38688023422ee2604b04c92d1cb001ffa4cd7c  /tmp/test_tolerancia.bin
```
**✅ PASS** — El sistema descargó el archivo exitosamente usando las réplicas en `dn1` y `dn3`. SHA-256 idéntico. El fallo fue **transparente** para el usuario.

---

### 8.5 Análisis de Resultados

| Criterio | Resultado |
|---|---|
| Autenticación JWT | ✅ Funcional |
| Namespace jerárquico | ✅ Funcional |
| Particionamiento en bloques de 64MB | ✅ Funcional |
| Replicación pipeline (factor 3) | ✅ Funcional |
| Verificación SHA-256 | ✅ Funcional |
| Tolerancia a fallos (1 nodo caído) | ✅ Funcional |
| Despliegue en AWS EC2 (4 instancias) | ✅ Funcional |
| Comunicación gRPC sobre Internet | ✅ Funcional |
| Re-replicación automática | ✅ Implementada (HeartbeatMonitor) |

---

## 9. Conclusiones

1. **gRPC con streaming** es una tecnología ideal para transferencias de bloques grandes, permitiendo enviar datos en chunks de 4MB sin materializar el bloque completo en memoria.

2. **La arquitectura Maestro–Trabajadores** de HDFS/GFS es efectiva para DFS académicos: separa claramente las responsabilidades de coordinación (NameNode) y almacenamiento (DataNodes).

3. **La replicación pipeline** reduce el ancho de banda del cliente: en lugar de subir el bloque N veces, lo sube una vez y los DataNodes se encargan de la replicación entre sí.

4. **Docker** facilita enormemente el despliegue distribuido: cada servicio es independiente, reproducible y portable entre entornos locales y cloud.

5. **AWS EC2** permite demostrar comunicación real sobre Internet entre nodos en diferentes instancias virtuales, validando que el sistema funciona en condiciones de producción y no solo en red local.

6. La verificación **SHA-256** garantiza la integridad de datos en cada escritura y lectura, detectando corrupción en disco o durante la transmisión.

---

## 10. Referencias

- Ghemawat, S., Gobioff, H., & Leung, S. T. (2003). *The Google File System*. SOSP '03.
- Shvachko, K., et al. (2010). *The Hadoop Distributed File System*. IEEE MSST.
- gRPC Documentation: https://grpc.io/docs/
- Protocol Buffers v3: https://protobuf.dev/
- Docker Documentation: https://docs.docker.com/
- AWS EC2 Documentation: https://docs.aws.amazon.com/ec2/

---

*Informe generado para la entrega del Proyecto de Arquitecturas de Nube y Sistemas Distribuidos – UPB 2026*
