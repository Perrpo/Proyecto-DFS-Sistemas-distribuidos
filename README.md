# 🗄️ MiniDFS – Sistema de Archivos Distribuidos por Bloques

> Proyecto de Arquitecturas de Nube y Sistemas Distribuidos – Facultad de Ingeniería de Sistemas e Informática  
> **Entrega:** 24 de mayo de 2026 | **Valor:** 20%

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
9. [Pruebas Integrales](#-pruebas-integrales)
10. [Tolerancia a Fallos](#-tolerancia-a-fallos)
11. [Cronograma de Implementación](#-cronograma-de-implementación)

---

## 📖 Descripción General

**MiniDFS** es una implementación minimalista de un Sistema de Archivos Distribuido (DFS) por bloques, similar en concepto a HDFS (Hadoop Distributed File System). Permite:

- **Almacenar archivos grandes** dividiéndolos automáticamente en bloques de 64 MB.
- **Distribuir los bloques** entre múltiples nodos de datos (DataNodes) con replicación pipeline.
- **Recuperar archivos** reconstruyéndolos desde los bloques con verificación de integridad SHA-256.
- **Tolerar fallos** de DataNodes gracias a la replicación (factor 3) y re-replicación automática.
- **Gestionar usuarios** con autenticación segura JWT y un sistema de archivos jerárquico virtual por usuario.

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

---

## 📁 Estructura del Proyecto

```
Proyecto DFS/
├── proto/
│   ├── dfs.proto              # Contrato gRPC (todos los servicios y mensajes)
│   ├── dfs_pb2.py             # Stubs de mensajes generados
│   ├── dfs_pb2_grpc.py        # Stubs de servicios generados
│   ├── generate_stubs.sh      # Script para regenerar stubs (Linux/Mac)
│   └── generate_stubs.bat     # Script para regenerar stubs (Windows)
│
├── namenode/
│   ├── server.py              # Servidor gRPC principal (todos los RPCs)
│   ├── auth_service.py        # Autenticación: register, login, validar JWT
│   ├── metadata_store.py      # Capa SQLite: users, files, blocks, locations, datanodes
│   ├── block_manager.py       # Asignación Round-Robin, detección sub-replicación
│   ├── heartbeat_monitor.py   # Daemon: detecta nodos caídos, programa re-replicación
│   ├── namespace_manager.py   # Sistema de archivos jerárquico virtual por usuario
│   ├── requirements.txt       # Dependencias Python del NameNode
│   └── Dockerfile             # Imagen Docker del NameNode
│
├── datanode/
│   ├── server.py              # Servidor gRPC (StoreBlock, RetrieveBlock, DeleteBlock)
│   ├── block_store.py         # Almacenamiento en disco + verificación SHA-256
│   ├── heartbeat_agent.py     # Envía heartbeats al NameNode cada 10s
│   ├── block_report_agent.py  # Reporta inventario de bloques cada 60s
│   ├── replication_agent.py   # Pipeline de replicación y re-replicación
│   ├── requirements.txt       # Dependencias Python del DataNode
│   └── Dockerfile             # Imagen Docker del DataNode
│
├── client/
│   ├── cli.py                 # CLI interactiva con todos los comandos
│   ├── auth_client.py         # Almacena token JWT en ~/.minidfs_token
│   ├── block_splitter.py      # Divide archivo en bloques de 64 MB con SHA-256
│   ├── block_assembler.py     # Reensambla bloques en archivo de salida
│   ├── block_transfer.py      # gRPC streaming con backoff exponencial y fallback
│   └── requirements.txt       # Dependencias Python del Cliente
│
├── docker-compose.yml         # Orquestación: 1 NameNode + 3 DataNodes
├── .env.example               # Plantilla de variables de entorno
├── config.py                  # Configuración global (variables de entorno)
├── init_test.sh               # Script de prueba integral automatizada
└── README.md                  # Este archivo
```

---

## 🛠️ Tecnologías Utilizadas

| Tecnología | Uso | Versión |
|---|---|---|
| **Python** | Lenguaje principal | 3.11+ |
| **gRPC / protobuf** | Comunicación entre servicios | grpcio 1.62+ |
| **SQLite** | Persistencia de metadatos en NameNode | stdlib |
| **PyJWT** | Tokens de autenticación JWT | 2.8+ |
| **bcrypt** | Hash de contraseñas de usuarios | 4.x |
| **Docker** | Contenedores por servicio | 24+ |
| **Docker Compose** | Orquestación local multi-nodo | v3.9 |
| **hashlib (SHA-256)** | Verificación de integridad de bloques | stdlib |

---

## 🚀 Instalación y Ejecución Local

### Requisitos Previos

- Python 3.11 o superior
- `pip` instalado
- (Opcional) Docker Desktop para modo contenedorizado

### 1. Clonar el Repositorio

```bash
git clone https://github.com/Perrpo/Proyecto-DFS-Sistemas-distribuidos.git
cd "Proyecto DFS"
```

### 2. Instalar Dependencias

```bash
# Dependencias del NameNode
pip install -r namenode/requirements.txt

# Dependencias del DataNode
pip install -r datanode/requirements.txt

# Dependencias del Cliente
pip install -r client/requirements.txt
```

### 3. Configurar Variables de Entorno (Opcional)

```bash
cp .env.example .env
# Editar .env con tus valores
```

### 4. Iniciar el NameNode

```bash
# Terminal 1
python namenode/server.py
```

### 5. Iniciar DataNodes (en terminales separadas)

```bash
# Terminal 2
NODE_ID=dn1 DATANODE_PORT=50061 DATANODE_HOST=localhost python datanode/server.py

# Terminal 3
NODE_ID=dn2 DATANODE_PORT=50062 DATANODE_HOST=localhost python datanode/server.py

# Terminal 4
NODE_ID=dn3 DATANODE_PORT=50063 DATANODE_HOST=localhost python datanode/server.py
```

### 6. Usar el Cliente

```bash
# Terminal 5 (desde la carpeta del proyecto)
cd client
python cli.py register miusuario micontraseña
python cli.py login miusuario micontraseña
python cli.py mkdir /mis-archivos
python cli.py put ../README.md /mis-archivos/README.md
python cli.py ls /mis-archivos
python cli.py get /mis-archivos/README.md ./README_descargado.md
```

---

## 💻 Uso de la CLI

### Autenticación

```bash
# Registrar un nuevo usuario
python client/cli.py register <usuario> <contraseña>

# Iniciar sesión (guarda token en ~/.minidfs_token)
python client/cli.py login <usuario> <contraseña>

# Cerrar sesión
python client/cli.py logout
```

### Gestión del Sistema de Archivos

```bash
# Listar archivos y directorios en una ruta
python client/cli.py ls [/ruta]          # Por defecto: /

# Crear directorio
python client/cli.py mkdir /ruta/dir

# Eliminar directorio vacío
python client/cli.py rmdir /ruta/dir
```

### Transferencia de Archivos

```bash
# Subir archivo al DFS (con replicación pipeline automática)
python client/cli.py put <ruta_local> <ruta_remota>

# Ejemplo: subir un video de 500 MB
python client/cli.py put ./video.mp4 /media/video.mp4

# Descargar archivo del DFS
python client/cli.py get <ruta_remota> [<ruta_local>]

# Ejemplo: descargar al directorio actual
python client/cli.py get /media/video.mp4 .

# Eliminar archivo del DFS
python client/cli.py rm <ruta_remota>
```

### Mensajes de Error Estándar

| Error | Descripción |
|---|---|
| `Error: sesión no válida` | Token JWT expirado o ausente. Ejecutar `login`. |
| `Error: archivo no encontrado` | El archivo no existe en el DFS. |
| `Error: archivo ya existe` | Ya hay un archivo con ese path. Usar `rm` primero. |
| `Error: no hay DataNodes suficientes` | Menos de 3 DataNodes activos para el factor de replicación. |
| `Error: bloque corrupto` | Checksum SHA-256 mismatch. Se reporta al NameNode y se intenta otra réplica. |

---

## 🐳 Ejecución con Docker Compose

```bash
# Levantar el stack completo (1 NameNode + 3 DataNodes)
docker compose up -d --build

# Ver logs en tiempo real
docker compose logs -f

# Ver logs de un servicio específico
docker compose logs -f namenode
docker compose logs -f datanode1

# Detener todo
docker compose down

# Detener y eliminar volúmenes (limpiar datos)
docker compose down --volumes
```

### Puertos Expuestos

| Servicio | Puerto | Descripción |
|---|---|---|
| NameNode | `50051` | gRPC – metadatos, auth, coordinación |
| DataNode 1 | `50061` | gRPC – almacenamiento de bloques |
| DataNode 2 | `50062` | gRPC – almacenamiento de bloques |
| DataNode 3 | `50063` | gRPC – almacenamiento de bloques |

### Conectar el Cliente al Stack Docker

```bash
# El cliente se conecta por defecto a localhost:50051
cd client
python cli.py login miusuario micontraseña
python cli.py put ./archivo.bin /datos/archivo.bin
```

---

## ☁️ Despliegue en AWS EC2

### Arquitectura de Despliegue en AWS

```
Internet
    │
    ▼
┌─────────────────────────────────────────────────────┐
│               AWS VPC (10.0.0.0/16)                 │
│                                                     │
│  ┌─────────────────────┐   ┌─────────────────────┐  │
│  │  EC2 – NameNode     │   │  EC2 – DataNode 1   │  │
│  │  t3.small           │   │  t3.medium          │  │
│  │  Puerto: 50051      │   │  Puerto: 50061      │  │
│  └─────────────────────┘   └─────────────────────┘  │
│                            ┌─────────────────────┐  │
│                            │  EC2 – DataNode 2   │  │
│                            │  t3.medium          │  │
│                            │  Puerto: 50062      │  │
│                            └─────────────────────┘  │
│                            ┌─────────────────────┐  │
│                            │  EC2 – DataNode 3   │  │
│                            │  t3.medium          │  │
│                            │  Puerto: 50063      │  │
│                            └─────────────────────┘  │
└─────────────────────────────────────────────────────┘
```

### Paso a Paso: Despliegue Manual en AWS EC2

#### 1. Crear Instancias EC2

En la consola de AWS Academy:

1. **AMI**: `Amazon Linux 2023` (o Ubuntu 22.04 LTS)
2. **Tipo de instancia**:
   - NameNode: `t3.small` (2 vCPU, 2 GB RAM)
   - DataNodes (×3): `t3.medium` (2 vCPU, 4 GB RAM)
3. **Security Group** – Abrir puertos de entrada (inbound rules):
   - `50051` (TCP) → para el NameNode (desde tu IP y las IPs de los DataNodes)
   - `50061-50063` (TCP) → para los DataNodes (desde las IPs de las otras instancias)
   - `22` (SSH) → desde tu IP

#### 2. Instalar Docker en cada instancia

```bash
# Conectarse via SSH
ssh -i "tu-key.pem" ec2-user@<IP_PUBLICA>

# Instalar Docker
sudo yum update -y
sudo yum install docker -y
sudo systemctl start docker
sudo systemctl enable docker
sudo usermod -aG docker ec2-user

# Instalar Docker Compose
sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" \
  -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose
```

#### 3. Clonar el Repositorio en cada instancia

```bash
sudo yum install git -y
git clone https://github.com/Perrpo/Proyecto-DFS-Sistemas-distribuidos.git
cd "Proyecto DFS"
```

#### 4. Configurar Variables de Entorno

**En la instancia del NameNode** – crear archivo `.env`:

```bash
cat > .env << 'EOF'
JWT_SECRET=mi-secreto-super-seguro-cambiar-en-produccion
NAMENODE_PORT=50051
NAMENODE_DB_PATH=/data/namenode.db
BLOCK_SIZE=67108864
REPLICATION_FACTOR=3
HEARTBEAT_INTERVAL=10
HEARTBEAT_TIMEOUT=30
LOG_LEVEL=INFO
EOF
```

**En cada instancia de DataNode** – crear `docker-compose.datanode.yml`:

```yaml
# Para DataNode 1 (reemplazar NODE_ID, DATANODE_HOST, DATANODE_PORT según corresponda)
version: '3.9'
services:
  datanode1:
    build:
      context: .
      dockerfile: datanode/Dockerfile
    container_name: minidfs-datanode1
    environment:
      - NODE_ID=dn1
      - NAMENODE_HOST=<IP_PRIVADA_NAMENODE>
      - NAMENODE_PORT=50051
      - DATANODE_HOST=<IP_PRIVADA_DATANODE1>
      - DATANODE_PORT=50061
      - DATANODE_STORAGE_PATH=/data/blocks
      - LOG_LEVEL=INFO
    ports:
      - "50061:50061"
    volumes:
      - ./data/blocks:/data/blocks
    restart: unless-stopped
```

#### 5. Iniciar los Servicios

**En la instancia del NameNode:**

```bash
# Iniciar solo el NameNode
docker compose up -d namenode

# Verificar
docker compose logs namenode
```

**En cada instancia de DataNode:**

```bash
# Iniciar el DataNode correspondiente
docker-compose -f docker-compose.datanode.yml up -d

# Verificar
docker-compose -f docker-compose.datanode.yml logs
```

#### 6. Verificar el Despliegue

Desde tu máquina local, configurar el cliente:

```bash
# Configurar la IP pública del NameNode
export NAMENODE_HOST=<IP_PUBLICA_NAMENODE>
export NAMENODE_PORT=50051

cd client
python cli.py register admin MiPassword123!
python cli.py login admin MiPassword123!
python cli.py ls /
```

#### 7. Despliegue Simplificado con Docker Compose en una Sola Instancia

Para propósitos académicos, es válido ejecutar todo en una sola instancia EC2 grande:

```bash
# En una instancia t3.xlarge o t3.2xlarge
docker compose up -d --build
docker compose ps
```

Esto levanta los 4 contenedores (NameNode + 3 DataNodes) en la misma instancia EC2, usando la red interna de Docker para la comunicación entre servicios.

---

## 🧪 Pruebas Integrales

### Prueba Automatizada Completa

```bash
# Requiere bash y Docker
chmod +x init_test.sh
./init_test.sh
```

El script ejecuta automáticamente:

1. ✅ Levantar stack Docker (1 NameNode + 3 DataNodes)
2. ✅ Crear usuario y autenticarse (JWT)
3. ✅ Crear un archivo de 130 MB con datos aleatorios
4. ✅ Subir el archivo al DFS con replicación pipeline
5. ✅ Listar archivos en el namespace virtual
6. ✅ Descargar el archivo y verificar SHA-256
7. ✅ Matar DataNode 1 (simular fallo)
8. ✅ Descargar de nuevo – verifica tolerancia a fallos
9. ✅ Esperar y verificar re-replicación automática en logs

### Prueba Manual Rápida (sin Docker)

```bash
# 1. Iniciar servicios localmente (4 terminales)
python namenode/server.py
NODE_ID=dn1 DATANODE_PORT=50061 python datanode/server.py
NODE_ID=dn2 DATANODE_PORT=50062 python datanode/server.py
NODE_ID=dn3 DATANODE_PORT=50063 python datanode/server.py

# 2. En otra terminal
cd client
python cli.py register andres pass123
python cli.py login andres pass123
python cli.py mkdir /test

# 3. Crear archivo de prueba y subirlo
python -c "import os; open('../test.bin','wb').write(os.urandom(10*1024*1024))"
python cli.py put ../test.bin /test/test.bin

# 4. Listar y descargar
python cli.py ls /test
python cli.py get /test/test.bin ../test_descargado.bin

# 5. Verificar integridad
python -c "
import hashlib
h1=hashlib.sha256(open('../test.bin','rb').read()).hexdigest()
h2=hashlib.sha256(open('../test_descargado.bin','rb').read()).hexdigest()
print('OK: Archivos idénticos' if h1==h2 else 'ERROR: Diferencia detectada')
"
```

---

## 🛡️ Tolerancia a Fallos

### Mecanismos Implementados

| Mecanismo | Descripción | Dónde |
|---|---|---|
| **Replicación Pipeline** | Cada bloque se replica en 3 DataNodes automáticamente durante el PUT | `datanode/server.py` + `datanode/replication_agent.py` |
| **Heartbeat Monitor** | El NameNode detecta DataNodes caídos (sin heartbeat en 30s) | `namenode/heartbeat_monitor.py` |
| **Re-replicación Automática** | Al detectar un nodo caído, el NameNode programa re-replicación de bloques sub-replicados | `namenode/heartbeat_monitor.py` + `namenode/block_manager.py` |
| **Verificación SHA-256** | Cada bloque se verifica al almacenarse y al leerse | `datanode/block_store.py` |
| **Fallback de Réplicas** | Si un DataNode falla durante GET, el cliente automáticamente intenta la siguiente réplica | `client/block_transfer.py` + `client/cli.py` |
| **Reporte de Corrupción** | Si un bloque está corrupto, el cliente lo reporta al NameNode para eliminar esa ubicación | `namenode/server.py` → `ReportCorruptBlock` |
| **Backoff Exponencial** | Reintentos con espera 1s → 2s → 4s ante errores transitorios | `client/block_transfer.py` + `datanode/replication_agent.py` |

### Escenarios de Fallo Cubiertos

```
Escenario 1: DataNode cae durante PUT
  → Los otros 2 DataNodes completan la replicación
  → El NameNode detecta el fallo en el siguiente ciclo de heartbeat
  → Re-replicación automática al siguiente DataNode activo

Escenario 2: DataNode cae durante GET
  → El cliente detecta el error (timeout gRPC)
  → Automáticamente intenta la siguiente réplica disponible
  → El usuario no experimenta error (transparente)

Escenario 3: Bloque corrupto en disco
  → verify_checksum() falla al leer
  → El cliente reporta ReportCorruptBlock al NameNode
  → NameNode elimina esa ubicación de los metadatos
  → El cliente intenta descargar desde otra réplica

Escenario 4: NameNode reinicia
  → Los DataNodes detectan la desconexión y reintentán con backoff
  → Al reconectarse, envían BlockReport completo
  → El NameNode reconcilia los metadatos automáticamente
```

---

## 📅 Cronograma de Implementación

| Día | Fecha | Commit | Alcance |
|---|---|---|---|
| **Día 1** | Jue 21 mayo | `feat: project foundation` ✅ | Estructura, Proto gRPC, NameNode completo (auth JWT, SQLite, namespace, heartbeat monitor), DataNode base, Docker |
| **Día 2** | Vie 22 mayo | `feat: core DFS operations` ✅ | PUT/GET de extremo a extremo, replicación pipeline, StoreBlock/RetrieveBlock en streaming, CLI put y get completos |
| **Día 3** | Sáb 23 mayo | `feat: fault tolerance & docker` ✅ | Verificación SHA-256 en BlockStore, Docker Compose con healthchecks, script de prueba integral, documentación completa, guía AWS EC2 |

---

## 👨‍💻 Autor

**Andrés Felipe Núñez Hernández**  
Facultad de Ingeniería de Sistemas e Informática  
Arquitecturas de Nube y Sistemas Distribuidos  
Universidad Pontificia Bolivariana – 2026

---

## 📄 Licencia

Proyecto académico – Uso educativo únicamente.
