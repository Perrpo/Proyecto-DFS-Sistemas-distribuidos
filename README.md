# 🗄️ MiniDFS – Sistema de Archivos Distribuidos por Bloques

> **Proyecto de Arquitecturas de Nube y Sistemas Distribuidos**  
> Facultad de Ingeniería de Sistemas e Informática – Universidad Pontificia Bolivariana  
> Fecha de entrega: 24 de mayo de 2026

---

## 📋 Tabla de Contenidos

1. [Descripción General](#-descripción-general)
2. [Arquitectura del Sistema](#-arquitectura-del-sistema)
3. [Estructura del Proyecto](#-estructura-del-proyecto)
4. [Tecnologías Utilizadas](#-tecnologías-utilizadas)
5. [Instalación y Ejecución Local](#-instalación-y-ejecución-local)
6. [Uso de la CLI](#-uso-de-la-cli)
7. [Ejecución con Docker Compose](#-ejecución-con-docker-compose)
8. [Despliegue en AWS EC2](#-despliegue-en-aws-ec2)
9. [Pruebas y Resultados](#-pruebas-y-resultados)
10. [Tolerancia a Fallos](#-tolerancia-a-fallos)

---

## 📖 Descripción General

**MiniDFS** es una implementación de un Sistema de Archivos Distribuido (DFS) por bloques inspirado en HDFS (Hadoop Distributed File System) y GFS (Google File System). Permite:

- **Almacenar archivos grandes** dividiéndolos automáticamente en bloques de 64 MB.
- **Distribuir los bloques** entre múltiples nodos de datos (DataNodes) con replicación pipeline automática (factor de replicación = 3).
- **Recuperar archivos** reconstruyéndolos desde los bloques con verificación de integridad SHA-256.
- **Tolerar fallos** de DataNodes gracias a la replicación y re-replicación automática orquestada por el NameNode.
- **Gestionar usuarios** con autenticación segura JWT y un sistema de archivos jerárquico virtual por usuario.

El sistema sigue una arquitectura **Maestro–Trabajadores** donde el NameNode actúa como coordinador central y los DataNodes como almacenamiento distribuido, comunicándose mediante **gRPC con streaming bidireccional** sobre Internet.

---

## 🏛️ Arquitectura del Sistema

```
┌────────────────────────────────────────────────────────────────────────┐
│                           CLIENTE (CLI)                                │
│  register | login | put | get | ls | mkdir | rmdir | rm | logout       │
└────────────┬──────────────────────────────────┬───────────────────────┘
             │ gRPC (metadatos + auth)           │ gRPC streaming (bloques)
             ▼                                  ▼
┌────────────────────────┐    ┌─────────────────────────────────────────┐
│      NAMENODE          │    │             DATANODES                   │
│  Puerto: 50051         │    │  DataNode 1: :50061   (NODE_ID=dn1)    │
│                        │    │  DataNode 2: :50062   (NODE_ID=dn2)    │
│  ● Auth Service (JWT)  │◄───│  DataNode 3: :50063   (NODE_ID=dn3)    │
│  ● Namespace Manager   │    │                                         │
│  ● Block Manager       │    │  ● BlockStore (disco + SHA-256)         │
│  ● Heartbeat Monitor   │    │  ● HeartbeatAgent (cada 10s)           │
│  ● Metadata SQLite     │    │  ● BlockReportAgent (cada 60s)         │
└────────────────────────┘    │  ● ReplicationAgent (pipeline)         │
                              └────────────┬────────────────────────────┘
                                           │ Pipeline de Replicación
                              DataNode1 ──► DataNode2 ──► DataNode3
```

### Flujo PUT (subir archivo)

```
Cliente                     NameNode                  DataNode 1,2,3
   │── PutFileRequest ──────►│                              │
   │◄─ BlockAssignments ──────│                              │
   │                          │                              │
   │── StoreBlock (streaming)─────────────────────────────►│dn1
   │                          │             dn1──pipeline──►│dn2
   │                          │             dn2──pipeline──►│dn3
   │── ConfirmBlock ──────────►│                              │
   │                          │ (actualiza SQLite)           │
```

### Flujo GET (descargar archivo)

```
Cliente                     NameNode                  DataNodes
   │── GetFileRequest ───────►│                              │
   │◄─ BlockLocations ─────────│                              │
   │                          │                              │
   │── RetrieveBlock ─────────────────────────────────────►│dn1
   │◄─ streaming chunks ──────────────────────────────────── │
   │   (fallback dn2,dn3 si dn1 falla o bloque corrupto)    │
```

### Heartbeat y Re-replicación

```
DataNode ──► Heartbeat (cada 10s) ──►  NameNode
DataNode ──► BlockReport (cada 60s) ──► NameNode
                                         │
                              HeartbeatMonitor detecta
                              nodo caído (>30s sin HB)
                                         │
                              Programa re-replicación
                                         │
                    DataNode activo ──► StoreBlock ──► Nuevo nodo
```

---

## 📁 Estructura del Proyecto

```
Proyecto DFS/
├── proto/
│   ├── dfs.proto              # Contrato gRPC (servicios y mensajes)
│   ├── dfs_pb2.py             # Stubs de mensajes generados
│   ├── dfs_pb2_grpc.py        # Stubs de servicios generados
│   ├── generate_stubs.sh      # Regenerar stubs (Linux/Mac)
│   └── generate_stubs.bat     # Regenerar stubs (Windows)
│
├── namenode/
│   ├── server.py              # Servidor gRPC (todos los RPCs)
│   ├── auth_service.py        # Autenticación JWT + bcrypt
│   ├── metadata_store.py      # SQLite: users, files, blocks, datanodes
│   ├── block_manager.py       # Asignación Round-Robin y sub-replicación
│   ├── heartbeat_monitor.py   # Daemon: detecta caídas, re-replicación
│   ├── namespace_manager.py   # Sistema de archivos jerárquico virtual
│   ├── requirements.txt
│   └── Dockerfile
│
├── datanode/
│   ├── server.py              # gRPC: StoreBlock, RetrieveBlock, DeleteBlock
│   ├── block_store.py         # Almacenamiento en disco + SHA-256
│   ├── heartbeat_agent.py     # Envía heartbeats al NameNode
│   ├── block_report_agent.py  # Reporta inventario de bloques
│   ├── replication_agent.py   # Pipeline de replicación
│   ├── requirements.txt
│   └── Dockerfile
│
├── client/
│   ├── cli.py                 # CLI completa (todos los comandos)
│   ├── auth_client.py         # Token JWT en ~/.minidfs_token
│   ├── block_splitter.py      # Divide archivos en bloques con SHA-256
│   ├── block_assembler.py     # Reensambla bloques en archivo
│   ├── block_transfer.py      # gRPC streaming + backoff exponencial
│   └── requirements.txt
│
├── docker-compose.yml         # Orquestación: 1 NameNode + 3 DataNodes
├── .env.example               # Variables de entorno de ejemplo
├── config.py                  # Configuración global
├── init_test.sh               # Script de prueba integral
└── README.md
```

---

## 🛠️ Tecnologías Utilizadas

| Tecnología | Uso | Versión |
|---|---|---|
| **Python** | Lenguaje principal | 3.11+ |
| **gRPC / protobuf** | Comunicación entre servicios (streaming) | grpcio 1.62+ |
| **SQLite** | Persistencia de metadatos en NameNode | stdlib |
| **PyJWT** | Tokens de autenticación JWT | 2.8+ |
| **bcrypt** | Hash seguro de contraseñas | 4.x |
| **Docker** | Contenedores por servicio | 24+ |
| **Docker Compose** | Orquestación local multi-nodo | v3.9 |
| **SHA-256 (hashlib)** | Verificación de integridad de bloques | stdlib |
| **AWS EC2** | Infraestructura en la nube (IaaS) | us-east-1 |

---

## 🚀 Instalación y Ejecución Local

### Requisitos

- Python 3.11+
- Docker Desktop (para modo contenedorizado)
- `pip` instalado

### 1. Clonar el repositorio

```bash
git clone https://github.com/Perrpo/Proyecto-DFS-Sistemas-distribuidos.git
cd "Proyecto DFS"
```

### 2. Instalar dependencias

```bash
pip install -r namenode/requirements.txt
pip install -r datanode/requirements.txt
pip install -r client/requirements.txt
```

### 3. Generar stubs gRPC

```bash
# Linux/Mac
bash proto/generate_stubs.sh

# Windows
proto\generate_stubs.bat
```

### 4. Iniciar manualmente (sin Docker)

```bash
# Terminal 1 – NameNode
python namenode/server.py

# Terminal 2 – DataNode 1
NODE_ID=dn1 DATANODE_PORT=50061 DATANODE_HOST=localhost python datanode/server.py

# Terminal 3 – DataNode 2
NODE_ID=dn2 DATANODE_PORT=50062 DATANODE_HOST=localhost python datanode/server.py

# Terminal 4 – DataNode 3
NODE_ID=dn3 DATANODE_PORT=50063 DATANODE_HOST=localhost python datanode/server.py
```

---

## 💻 Uso de la CLI

### Autenticación

```bash
python client/cli.py register <usuario> <contraseña>
python client/cli.py login    <usuario> <contraseña>
python client/cli.py logout
```

### Gestión del sistema de archivos

```bash
python client/cli.py ls    [/ruta]          # Listar archivos y directorios
python client/cli.py mkdir /ruta/dir        # Crear directorio
python client/cli.py rmdir /ruta/dir        # Eliminar directorio vacío
python client/cli.py rm    /ruta/archivo    # Eliminar archivo
```

### Transferencia de archivos

```bash
# Subir archivo (replicación pipeline automática a 3 DataNodes)
python client/cli.py put ./video.mp4 /media/video.mp4

# Descargar archivo (fallback automático a réplicas disponibles)
python client/cli.py get /media/video.mp4 ./video_local.mp4
```

### Mensajes de error

| Error | Causa |
|---|---|
| `Error: sesión no válida` | Token JWT expirado — ejecutar `login` |
| `Error: archivo no encontrado` | El path no existe en el DFS |
| `Error: no hay DataNodes suficientes` | Menos de 3 DataNodes activos |
| `Error: bloque corrupto` | SHA-256 mismatch — se intenta réplica alternativa |

---

## 🐳 Ejecución con Docker Compose

```bash
# Levantar el stack completo
docker compose up -d --build

# Ver logs
docker compose logs -f namenode
docker compose logs -f datanode1

# Detener
docker compose down

# Limpiar volúmenes (eliminar datos)
docker compose down --volumes
```

### Puertos expuestos

| Servicio | Puerto | Descripción |
|---|---|---|
| NameNode | `50051` | gRPC – metadatos, auth, coordinación |
| DataNode 1 | `50061` | gRPC – almacenamiento de bloques |
| DataNode 2 | `50062` | gRPC – almacenamiento de bloques |
| DataNode 3 | `50063` | gRPC – almacenamiento de bloques |

---

## ☁️ Despliegue en AWS EC2

El sistema fue desplegado y probado en 4 instancias EC2 independientes en la región `us-east-1`:

| Instancia | Tipo | IP Privada | Servicio |
|---|---|---|---|
| NameNode | t3.small | 172.31.0.165 | NameNode gRPC :50051 |
| DataNode 1 | t3.small | 172.31.5.1 | DataNode gRPC :50061 |
| DataNode 2 | t3.small | 172.31.9.175 | DataNode gRPC :50062 |
| DataNode 3 | t3.small | 172.31.9.76 | DataNode gRPC :50063 |

### Security Group (proyecto_dfs)

| Puerto | Protocolo | Origen | Servicio |
|---|---|---|---|
| 22 | TCP | 0.0.0.0/0 | SSH |
| 50051 | TCP | 0.0.0.0/0 | NameNode gRPC |
| 50061 | TCP | 0.0.0.0/0 | DataNode 1 gRPC |
| 50062 | TCP | 0.0.0.0/0 | DataNode 2 gRPC |
| 50063 | TCP | 0.0.0.0/0 | DataNode 3 gRPC |

### Pasos de despliegue

```bash
# En cada instancia EC2 (Ubuntu 26.04 LTS)
sudo apt install -y docker.io docker-compose git python3.14-venv
sudo systemctl start docker && sudo systemctl enable docker
sudo usermod -aG docker ubuntu

git clone https://github.com/Perrpo/Proyecto-DFS-Sistemas-distribuidos.git
cd Proyecto-DFS-Sistemas-distribuidos

# Corregir NAMENODE_HOST en DataNodes
sed -i 's/NAMENODE_HOST=namenode/NAMENODE_HOST=172.31.0.165/g' docker-compose.yml

# NameNode
docker compose up -d namenode

# DataNode 1 (en su instancia, con IP real)
sed -i 's/DATANODE_HOST=datanode1/DATANODE_HOST=172.31.5.1/g' docker-compose.yml
docker compose up -d datanode1 --no-deps
```

---

## 🧪 Pruebas y Resultados

### Prueba 1: Registro, Login y Namespace

```
$ python cli.py register andres pass123
Usuario registrado exitosamente

$ python cli.py login andres pass123
Autenticación exitosa

$ python cli.py mkdir /test
Directorio creado

$ python cli.py ls /
d  /test/
```

### Prueba 2: PUT con replicación pipeline

```
$ python cli.py put /tmp/test.bin /test/test.bin
Subiendo: /tmp/test.bin (5.0 MB, 1 bloque(s))
  → /test/test.bin
  Bloque 1/1: 5.0 MB → dn2 (pipeline: ['dn3', 'dn1']) ... OK

✓ Archivo subido exitosamente: /test/test.bin
  1/1 bloques confirmados | file_id=548492e7-9f9c-4ee3-8bc0-22b4d939a86c
```

### Prueba 3: GET y verificación SHA-256

```
$ python cli.py get /test/test.bin /tmp/test_descargado.bin
Descargando: /test/test.bin (5.0 MB, 1 bloque(s))
  Bloque 1/1: réplicas=['dn1', 'dn2', 'dn3'] ... OK
✓ Archivo descargado exitosamente (5.0 MB)

$ sha256sum /tmp/test.bin /tmp/test_descargado.bin
5c3c26904f888b5970cfdce8cb38688023422ee2604b04c92d1cb001ffa4cd7c  /tmp/test.bin
5c3c26904f888b5970cfdce8cb38688023422ee2604b04c92d1cb001ffa4cd7c  /tmp/test_descargado.bin
```
**✅ SHA-256 idéntico — integridad perfecta**

### Prueba 4: Tolerancia a fallos (DataNode 2 caído)

```
# DataNode 2 detenido
$ python cli.py get /test/test.bin /tmp/test_tolerancia.bin
  Bloque 1/1: réplicas=['dn1', 'dn2', 'dn3'] ... OK
✓ Archivo descargado exitosamente (5.0 MB)

$ sha256sum /tmp/test.bin /tmp/test_tolerancia.bin
5c3c26904f888b5970cfdce8cb38688023422ee2604b04c92d1cb001ffa4cd7c  /tmp/test.bin
5c3c26904f888b5970cfdce8cb38688023422ee2604b04c92d1cb001ffa4cd7c  /tmp/test_tolerancia.bin
```
**✅ GET exitoso con un DataNode caído gracias al fallback de réplicas**

---

## 🛡️ Tolerancia a Fallos

### Mecanismos implementados

| Mecanismo | Descripción | Módulo |
|---|---|---|
| **Replicación Pipeline** | Cada bloque se replica a 3 DataNodes durante el PUT | `datanode/replication_agent.py` |
| **Heartbeat Monitor** | Detecta DataNodes caídos (sin HB en 30s) | `namenode/heartbeat_monitor.py` |
| **Re-replicación automática** | Replica bloques sub-replicados al detectar fallo | `namenode/block_manager.py` |
| **Verificación SHA-256** | Integridad verificada al escribir y al leer | `datanode/block_store.py` |
| **Fallback de réplicas** | GET intenta réplicas alternativas si una falla | `client/block_transfer.py` |
| **Reporte de corrupción** | Bloques corruptos reportados al NameNode | `namenode/server.py` |
| **Backoff exponencial** | Reintentos con espera 1s→2s→4s | `client/block_transfer.py` |

### Escenarios de fallo cubiertos

```
Escenario 1: DataNode cae durante PUT
  → Replicación pipeline al siguiente nodo disponible
  → NameNode detecta caída y programa re-replicación

Escenario 2: DataNode cae durante GET
  → Cliente detecta error, intenta siguiente réplica
  → Transparente para el usuario

Escenario 3: Bloque corrupto en disco (SHA-256 mismatch)
  → Cliente reporta ReportCorruptBlock al NameNode
  → NameNode elimina esa ubicación, cliente usa otra réplica

Escenario 4: NameNode reinicia
  → DataNodes reconectan con backoff exponencial
  → Envían BlockReport completo al reconectarse
```

---

## 👨‍💻 Autor

**Andrés Felipe Núñez Hernández**  
Facultad de Ingeniería de Sistemas e Informática  
Arquitecturas de Nube y Sistemas Distribuidos  
Universidad Pontificia Bolivariana – 2026

---

## 📄 Licencia

Proyecto académico – Uso educativo únicamente.
