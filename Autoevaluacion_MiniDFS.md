# Autoevaluación – Proyecto MiniDFS
## Sistema de Archivos Distribuidos por Bloques
**Estudiante:** Andrés Felipe Núñez Hernández  
**Asignatura:** Arquitecturas de Nube y Sistemas Distribuidos  
**Universidad Pontificia Bolivariana – 2026**

---

## 1. Breve descripción de la actividad

El proyecto consistió en diseñar e implementar desde cero un sistema de archivos distribuido por bloques (DFS) minimalista, inspirado en HDFS y GFS, que permitiera almacenar y recuperar archivos grandes de forma distribuida entre múltiples nodos, con replicación automática y tolerancia a fallos.

---

### A. Aspectos que cumplí y desarrollé

Considero que logré cumplir la gran mayoría de los requerimientos propuestos. A continuación detallo qué implementé y cómo:

**Requerimientos funcionales cumplidos:**

- **Operación `put`:** Implementé la subida de archivos con particionamiento automático en bloques de 64 MB configurables. El archivo se divide en bloques, cada uno con su checksum SHA-256, y se distribuye entre los DataNodes usando asignación Round-Robin.

- **Operación `get`:** El cliente recupera los bloques del DFS ordenados por índice y los reconstruye en el archivo original. Si un DataNode falla durante la descarga, el cliente automáticamente intenta la siguiente réplica disponible.

- **Operaciones `ls`, `mkdir`, `rmdir`, `rmdir -r`, `rm`:** Implementé un sistema de archivos jerárquico virtual por usuario. Cada usuario tiene su propio espacio de nombres y puede crear directorios, listar contenido y eliminar archivos. La operación `rmdir` elimina directorios vacíos, y con la flag `-r` elimina recursivamente el directorio y todo su contenido en cascada, incluyendo archivos y subdirectorios anidados.

- **Autenticación básica (user/pass):** Los usuarios se registran con contraseña hasheada con bcrypt. El login genera un token JWT con expiración de 24 horas que se adjunta a cada solicitud al NameNode.

- **Replicación mínima en 2 o más DataNodes:** Implementé replicación pipeline con factor 3. Cada bloque se almacena en 3 DataNodes distintos. El cliente sube el bloque al nodo primario y ese nodo lo replica hacia los siguientes en cadena.

- **NameNode central con metadatos:** El NameNode gestiona toda la información sobre dónde está cada bloque, qué archivos existen y qué usuarios están registrados. Usa SQLite como motor de persistencia.

- **Múltiples DataNodes:** Desplegué 3 DataNodes independientes, cada uno en su propia instancia EC2 de AWS.

- **Comunicación sobre Internet:** Todo el sistema se comunica mediante gRPC sobre HTTP/2, con los servicios expuestos públicamente a través de los puertos correspondientes.

- **Tolerancia a fallos básica:** Si un DataNode cae, el sistema detecta el fallo mediante el mecanismo de Heartbeat y continúa sirviendo archivos desde las réplicas restantes. Además, el HeartbeatMonitor programa la re-replicación automática de los bloques afectados.

- **Verificación de integridad:** Cada bloque tiene su SHA-256 verificado en el momento de escritura y lectura. Si un bloque está corrupto, el sistema lo reporta al NameNode y busca una réplica sana.

- **Ejecución en Docker:** Cada componente (NameNode y DataNodes) está empaquetado en su propio contenedor Docker con Dockerfile propio, y se orquestan con Docker Compose.

- **Despliegue en AWS EC2:** El sistema fue desplegado y probado en 4 instancias EC2 reales en la región us-east-1 (1 NameNode t3.small + 3 DataNodes t3.medium).

- **Comando `status` (observabilidad del clúster):** Implementé un nuevo RPC `GetClusterStatus` en el NameNode y el comando `status` en la CLI. Este comando muestra en tiempo real el estado de todos los DataNodes: si están activos o caídos, cuántos bloques tiene almacenados cada uno, el espacio disponible en disco y el timestamp del último heartbeat recibido. Esto permite verificar visualmente que los bloques están efectivamente distribuidos entre los tres nodos, que es uno de los aspectos más difíciles de demostrar en un DFS.

**Porcentaje de cumplimiento estimado: 100%**

---

### B. Aspectos que NO cumplí o dejé incompletos

Siendo honesto, aunque logré implementar todo lo que la rúbrica pedía, hay algunas simplificaciones respecto a lo que un sistema de producción real requeriría:

- **No implementé actualización parcial de bloques:** El sistema sigue el modelo WORM (Write Once Read Many) al igual que HDFS. No es posible modificar un bloque ya almacenado; si el usuario quiere actualizar un archivo, debe eliminarlo y volver a subirlo completo. Esta no es una limitación del diseño sino una decisión deliberada alineada con el enunciado del proyecto.

- **El `rm` no libera espacio en disco inmediatamente:** Cuando elimino un archivo desde el NameNode, los metadatos se borran de SQLite, pero la orden de borrar los bloques físicos de cada DataNode llega en el próximo ciclo de BlockReport (hasta 60 segundos después). En producción, esto se manejaría con un proceso de limpieza diferida más agresivo.

- **No implementé cuotas por usuario:** Cualquier usuario autenticado puede subir archivos sin límite de almacenamiento. En un sistema real esto sería un problema crítico de gobernanza.

- **La interfaz de cliente es solo CLI:** No desarrollé una API REST o SDK programático como alternativa más amigable para integraciones con otros sistemas. Sin embargo, el servicio gRPC del NameNode funciona como API programática formal gracias al contrato en `dfs.proto`.

- **NameNode es un único punto de falla (SPOF):** Si el NameNode cae, el sistema completo deja de funcionar. HDFS resuelve esto con HA NameNode (activo/pasivo). Para este proyecto académico, esa complejidad quedó fuera del alcance.

---

## 2. Información general de diseño de alto nivel, arquitectura, patrones y mejores prácticas

El sistema sigue la arquitectura **Maestro–Trabajadores** (Master–Workers), que es el mismo patrón que usan HDFS y GFS. Esta decisión fue deliberada porque separa claramente el plano de control del plano de datos:

- El **NameNode** (Maestro) solo gestiona metadatos y coordinación, nunca toca los datos reales.
- Los **DataNodes** (Trabajadores) solo almacenan y sirven bloques, sin necesitar conocer el contexto global del sistema.
- El **Cliente** interactúa directamente con el NameNode para obtener ubicaciones y luego se conecta directamente a los DataNodes para transferir bloques. Esto evita que el NameNode sea cuello de botella en el plano de datos.

**Patrones y mejores prácticas que apliqué:**

- **Separación de responsabilidades:** Cada módulo tiene una sola función clara. Por ejemplo, `block_store.py` solo se encarga del almacenamiento físico, `heartbeat_monitor.py` solo monitorea nodos, y `auth_service.py` solo gestiona autenticación.

- **Replicación pipeline:** En lugar de que el cliente suba el bloque N veces (una por réplica), lo sube una sola vez al nodo primario y los DataNodes se encargan de replicarlo entre sí en cadena. Esto reduce el ancho de banda consumido por el cliente.

- **Backoff exponencial:** Cuando el cliente falla al conectarse a un DataNode, espera 1 segundo, luego 2, luego 4, antes de rendirse. Esto evita tormentas de reintentos.

- **Contratos gRPC con Protocol Buffers:** Toda la comunicación está definida en `proto/dfs.proto`, que actúa como contrato formal entre servicios. Esto facilita el versionado y la evolución de la API. Gracias a este contrato, pude agregar el nuevo RPC `GetClusterStatus` (para el comando `status`) y el campo `recursive` en `RemoveDirRequest` (para `rmdir -r`) sin romper nada de lo que ya existía.

- **Verificación de integridad en ambos extremos:** El SHA-256 de cada bloque se calcula en el cliente al subir y se verifica en el DataNode al guardar, y viceversa al descargar. Esto detecta corrupción tanto en disco como en tránsito.

- **Thread daemons para tareas de fondo:** El HeartbeatMonitor, el HeartbeatAgent y el BlockReportAgent son threads daemon que corren en segundo plano sin bloquear el servicio principal.

- **Observabilidad integrada:** El comando `status` —respaldado por el RPC `GetClusterStatus`— permite ver en cualquier momento el estado del clúster: DataNodes activos/inactivos, bloques por nodo y espacio disponible. Esto no solo es útil para operar el sistema, sino que también permite demostrar visualmente que la distribución de bloques funciona correctamente.

---

## 3. Ambiente de desarrollo y técnico

### Lenguaje y librerías

| Componente | Versión |
|---|---|
| Python | 3.11 (contenedores) / 3.14 (AWS Ubuntu 26.04) |
| grpcio | 1.80.0 |
| grpcio-tools | 1.80.0 |
| protobuf | 6.33.6 |
| PyJWT | 2.13.0 |
| bcrypt | 5.0.0 |
| Docker | 29.1.3 |
| Docker Compose v2 | 2.40.3 |
| SQLite | stdlib de Python |

### A. Cómo se compila y ejecuta

**Con Docker Compose (recomendado):**
```bash
git clone https://github.com/Perrpo/Proyecto-DFS-Sistemas-distribuidos.git
cd Proyecto-DFS-Sistemas-distribuidos

# Levantar todo el stack
docker compose up -d --build

# Usar el cliente (desde otra terminal)
cd client
pip install -r requirements.txt
python cli.py register usuario contrasena
python cli.py login usuario contrasena
python cli.py put ./archivo.txt /dfs/archivo.txt
python cli.py get /dfs/archivo.txt ./descargado.txt
```

**Sin Docker (ejecución nativa):**
```bash
# Terminal 1 - NameNode
python namenode/server.py

# Terminal 2, 3, 4 - DataNodes
NODE_ID=dn1 DATANODE_PORT=50061 DATANODE_HOST=localhost python datanode/server.py
NODE_ID=dn2 DATANODE_PORT=50062 DATANODE_HOST=localhost python datanode/server.py
NODE_ID=dn3 DATANODE_PORT=50063 DATANODE_HOST=localhost python datanode/server.py
```

Los stubs gRPC se generan automáticamente durante el build de Docker. Para generarlos manualmente:
```bash
python -m grpc_tools.protoc -I . --python_out=. --grpc_python_out=. proto/dfs.proto
```

### B. Configuración de parámetros

Todos los parámetros se configuran mediante **variables de entorno** definidas en `config.py`:

| Variable | Valor por defecto | Descripción |
|---|---|---|
| `NAMENODE_HOST` | `namenode` | Hostname del NameNode (en Docker) o IP pública (en EC2) |
| `NAMENODE_PORT` | `50051` | Puerto gRPC del NameNode |
| `DATANODE_HOST` | `datanode1/2/3` | Hostname que el DataNode registra ante el NameNode |
| `DATANODE_PORT` | `50061/62/63` | Puerto gRPC del DataNode |
| `NODE_ID` | `dn1/dn2/dn3` | Identificador único del DataNode |
| `BLOCK_SIZE` | `67108864` (64 MB) | Tamaño de bloque en bytes |
| `REPLICATION_FACTOR` | `3` | Número de réplicas por bloque |
| `JWT_SECRET` | `changeme` | Clave secreta para firmar tokens JWT |
| `HEARTBEAT_INTERVAL` | `10` | Segundos entre heartbeats |
| `HEARTBEAT_TIMEOUT` | `30` | Segundos sin HB para considerar nodo caído |
| `BLOCK_REPORT_INTERVAL` | `60` | Segundos entre BlockReports |

Para el despliegue en AWS EC2, se modifica `NAMENODE_HOST` con la IP privada real del NameNode y `DATANODE_HOST` con la IP privada de cada instancia DataNode, de modo que los componentes puedan comunicarse entre sí a través de la red de la VPC.

---

## 4. Otra información relevante

**Decisión sobre el motor de almacenamiento:** Opté por SQLite para los metadatos del NameNode porque es simple, no requiere un servidor externo, y para la escala de este proyecto académico es más que suficiente. En un sistema de producción real como HDFS, los metadatos se mantienen en memoria RAM con un log de ediciones en disco.

**Por qué gRPC en lugar de REST:** gRPC con Protocol Buffers serializa los datos de forma binaria, lo que es significativamente más eficiente que JSON/REST para transferir bloques de datos grandes. Además, el soporte nativo para streaming (client-streaming y server-streaming) es exactamente lo que necesitamos para subir y bajar bloques en chunks sin cargar todo en memoria.

**Desafío del despliegue multi-instancia:** El mayor reto técnico fue que el `docker-compose.yml` usa nombres de servicio internos de Docker (`namenode`, `datanode1`, etc.) que no resuelven entre instancias EC2 distintas. La solución fue usar `sed` para reemplazar los hostnames con las IPs privadas reales antes de levantar cada servicio, y usar `--no-deps` para evitar que Docker Compose intentara levantar servicios dependientes en la misma instancia.

**Funcionalidades añadidas en la fase final:** Durante la última etapa del proyecto agregué dos mejoras que considero importantes. La primera es el comando `status`, que implementé junto con un nuevo RPC `GetClusterStatus` en el NameNode. Este comando me permitió demostrar visualmente durante el video que los bloques están distribuidos entre los tres DataNodes, algo que sin este comando hubiera sido muy difícil de evidenciar. La segunda es `rmdir -r`, que añadí porque me parecía una limitación real del sistema: no poder borrar un directorio con contenido es algo que cualquier usuario esperaría que funcionara. Ambas mejoras tocaron el proto, el NameNode, el MetadataStore y la CLI, por lo que fue un buen ejercicio de extensión del sistema sin romper nada existente.

**Trabajo individual:** Este proyecto lo desarrollé completamente solo, lo que implicó diseñar, implementar, depurar y desplegar todos los componentes del sistema. Organicé el trabajo en varias jornadas de desarrollo iterativo, comenzando por los cimientos del sistema (contratos gRPC y esqueleto de servicios), luego la lógica de negocio (put/get con replicación), después la robustez (integridad SHA-256, healthchecks en Docker, y despliegue en AWS), y finalmente la capa de observabilidad y usabilidad (status, rmdir -r).

---

## 5. Referencias

1. **Paper fundacional:** Ghemawat, S., Gobioff, H., & Leung, S. T. (2003). *The Google File System*. Proceedings of the 19th ACM Symposium on Operating Systems Principles (SOSP '03). https://research.google/pubs/the-google-file-system/

2. **Paper fundacional:** Shvachko, K., Kuang, H., Radia, S., & Chansler, R. (2010). *The Hadoop Distributed File System*. 2010 IEEE 26th Symposium on Mass Storage Systems and Technologies (MSST). https://ieeexplore.ieee.org/document/5496972

3. **Documentación de gRPC:** Google. *gRPC – A high performance, open source universal RPC framework*. https://grpc.io/docs/

4. **Documentación de Protocol Buffers:** Google. *Protocol Buffers v3 Language Guide*. https://protobuf.dev/programming-guides/proto3/

5. **Documentación de PyJWT:** https://pyjwt.readthedocs.io/en/stable/

6. **Documentación de bcrypt:** https://pypi.org/project/bcrypt/

7. **Docker Compose documentation:** https://docs.docker.com/compose/

8. **AWS EC2 User Guide:** https://docs.aws.amazon.com/ec2/

9. **Wikipedia – Google File System (contexto teórico):** https://es.wikipedia.org/wiki/Google_File_System

10. **Wikipedia – HDFS (contexto teórico):** https://es.wikipedia.org/wiki/Hadoop_Distributed_File_System

> **Nota sobre reutilización de código:** Todo el código de este proyecto fue escrito por mí desde cero para este proyecto académico. No se reutilizó código de proyectos externos. Las librerías utilizadas (gRPC, PyJWT, bcrypt) son dependencias estándar de Python, instaladas mediante pip, y su uso sigue la documentación oficial de cada una.
