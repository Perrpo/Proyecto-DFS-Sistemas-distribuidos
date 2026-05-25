# 🎬 Guión – Video Demostración MiniDFS
## Duración objetivo: 12–15 minutos | Solo presentador: Andrés Núñez

---

## 📋 ESTRUCTURA GENERAL

| Segmento | Tema | Tiempo |
|---|---|---|
| 1 | Introducción y presentación | 1 min |
| 2 | Arquitectura del sistema | 2 min |
| 3 | Recorrido por el código | 3 min |
| 4 | Demo en vivo – AWS EC2 (put, get, sha256) | 5 min |
| 4b | Demo nuevas funciones (status, rmdir -r) | 2 min |
| 5 | Tolerancia a fallos | 2 min |
| 6 | Cierre | 1 min |

---

## 🎙️ GUIÓN COMPLETO

---

### SEGMENTO 1 – INTRODUCCIÓN (1 min)
> 📺 **Pantalla:** Cámara frontal + diapositiva con el nombre del proyecto

**[Hablar mirando a cámara]**

> *"Hola, soy Andrés Núñez, estudiante de Ingeniería de Sistemas de la Universidad Pontificia Bolivariana. En este video voy a presentar MiniDFS, un Sistema de Archivos Distribuidos por Bloques que desarrollé para la materia de Arquitecturas de Nube y Sistemas Distribuidos.*
>
> *MiniDFS es una implementación minimalista inspirada en HDFS, el sistema de archivos de Hadoop, y en GFS, el sistema de archivos de Google. El objetivo es almacenar archivos grandes de forma distribuida entre múltiples nodos, con replicación automática y tolerancia a fallos.*
>
> *Durante esta demostración voy a mostrarles la arquitectura del sistema, el código implementado, y una ejecución en vivo sobre cuatro instancias de AWS EC2 donde el sistema está corriendo en producción."*

---

### SEGMENTO 2 – ARQUITECTURA (2 min)
> 📺 **Pantalla:** Compartir pantalla → abrir el README.md o el informe Word con los diagramas

**[Señalar el diagrama de arquitectura]**

> *"La arquitectura sigue el patrón Maestro–Trabajadores. Tenemos tres componentes principales:*
>
> *Primero, el **NameNode**: es el cerebro del sistema. Gestiona todos los metadatos, la autenticación de usuarios con tokens JWT, el espacio de nombres de archivos y coordina dónde se almacena cada bloque. Corre en el puerto 50051.*
>
> *Segundo, los **DataNodes**: son los trabajadores de almacenamiento. En nuestro caso tenemos tres: dn1, dn2 y dn3, corriendo en los puertos 50061, 50062 y 50063. Cada uno almacena bloques en disco con verificación de integridad SHA-256.*
>
> *Tercero, el **Cliente**: una interfaz de línea de comandos con la que el usuario sube, descarga y gestiona sus archivos.*
>
> *La comunicación entre todos los componentes se hace mediante **gRPC con Protocol Buffers**, que es mucho más eficiente que REST para transferencia de datos en streaming.*
>
> *[Señalar flecha de replicación pipeline]*
>
> *Un punto clave del diseño es la **replicación pipeline**: cuando el cliente sube un bloque, lo envía solo al primer DataNode. Ese nodo lo reenvía al segundo, y el segundo al tercero. Así el cliente solo usa su ancho de banda una vez, y los DataNodes se encargan de la replicación entre sí, igual que hace HDFS."*

---

### SEGMENTO 3 – RECORRIDO POR EL CÓDIGO (3 min)
> 📺 **Pantalla:** VS Code con el proyecto abierto

**[Abrir VS Code, mostrar la estructura de carpetas]**

> *"Veamos rápidamente la estructura del proyecto. Tenemos cuatro carpetas principales.*
>
> *En **proto/** está el contrato gRPC, el archivo dfs.proto que define todos los servicios y mensajes del sistema. Este es el punto de verdad de toda la comunicación."*

**[Abrir proto/dfs.proto]**

> *"Aquí podemos ver por ejemplo el servicio StoreBlock, que es client-streaming, lo que significa que el cliente envía el bloque en chunks de 4 megabytes. Y RetrieveBlock es server-streaming, el DataNode le envía el bloque al cliente en chunks de 1 megabyte."*

**[Abrir namenode/heartbeat_monitor.py]**

> *"En el NameNode, el componente más interesante es el HeartbeatMonitor. Es un thread daemon que corre en segundo plano y monitorea si los DataNodes siguen vivos. Cada DataNode envía un heartbeat cada 10 segundos. Si pasan más de 30 segundos sin heartbeat, el NameNode marca ese nodo como inactivo y automáticamente busca los bloques que quedaron sub-replicados para replicarlos en otros nodos disponibles. Esto es lo que nos da la tolerancia a fallos."*

**[Abrir datanode/block_store.py]**

> *"En el DataNode, el BlockStore es donde se almacenan físicamente los bloques en disco. Cada vez que se escribe un bloque, se calcula su SHA-256 y se guarda junto al bloque. Al momento de leer, se vuelve a calcular y se compara. Si no coinciden, el bloque está corrupto y el sistema automáticamente busca una réplica en otro nodo."*

**[Abrir client/cli.py]**

> *"Y finalmente el cliente. La CLI implementa todos los comandos requeridos: put, get, ls, mkdir, rmdir, rm, register y login. La autenticación usa JWT, por lo que el token se persiste localmente y se envía en cada solicitud al NameNode."*

---

### SEGMENTO 4 – DEMO EN VIVO AWS EC2 (5 min)
> 📺 **Pantalla:** 4 terminales SSH abiertas + PowerShell local

**[Mostrar las 4 terminales, cada una conectada a una instancia EC2]**

> *"Bien, vamos a la parte más importante: la demostración en vivo. Tengo cuatro instancias de AWS EC2 corriendo en us-east-1. En esta terminal está el NameNode con IP privada 172.31.0.165. Y en estas tres están los DataNodes."*

**[Ir a terminal del NameNode]**

```bash
docker ps
```
> *"Vemos que el contenedor minidfs-namenode está corriendo y en estado healthy. Puerto 50051 expuesto."*

**[Ir a terminal DataNode 1]**
```bash
docker ps
docker logs minidfs-datanode1 --tail 5
```
> *"DataNode 1 está conectado al NameNode en 172.31.0.165 y enviando BlockReports periódicamente."*

**[Ir a terminal del NameNode, ver logs]**
```bash
docker logs minidfs-namenode --tail 15
```
> *"Aquí en los logs del NameNode vemos los BlockReports que llegan de dn1, dn2 y dn3. El clúster está completamente operativo."*

---

**[Ir a la terminal con el cliente activo (venv activado)]**

```bash
cd ~/Proyecto-DFS-Sistemas-distribuidos/client
source ~/venv-dfs/bin/activate
```

> *"Ahora vamos a usar el cliente. Primero registro un usuario y me autentico:"*

```bash
python cli.py register andres pass123
python cli.py login andres pass123
```
> *"Usuario registrado y autenticado. El NameNode generó un token JWT que el cliente guarda localmente."*

```bash
python cli.py ls /
```
> *"El sistema de archivos está vacío. Creemos un directorio:"*

```bash
python cli.py mkdir /demo
python cli.py ls /
```

---

> *"Ahora viene la parte más interesante: subir un archivo. Voy a crear un archivo de prueba de 10 megabytes con datos aleatorios:"*

```bash
dd if=/dev/urandom of=/tmp/archivo_demo.bin bs=1M count=10
ls -lh /tmp/archivo_demo.bin
```

```bash
python cli.py put /tmp/archivo_demo.bin /demo/archivo_demo.bin
```

> *"Observen la salida: el archivo fue dividido en bloques y el bloque fue enviado a un DataNode primario. Vemos el pipeline de replicación: el bloque viajó de dn1 a dn2 a dn3 automáticamente. 1 de 1 bloques confirmados."*

```bash
python cli.py ls /demo
```
> *"Podemos ver el archivo en el DFS con su tamaño correcto."*

---

> *"Ahora lo descargamos:"*

```bash
python cli.py get /demo/archivo_demo.bin /tmp/archivo_descargado.bin
```

> *"Descarga exitosa. Y lo más importante: verificamos la integridad con SHA-256:"*

```bash
sha256sum /tmp/archivo_demo.bin /tmp/archivo_descargado.bin
```

> *"Los dos hashes son idénticos. El archivo que descargamos es exactamente igual al que subimos, byte por byte. La integridad está garantizada."*

---

### SEGMENTO 4b – NUEVAS FUNCIONALIDADES (2 min)
> 📺 **Pantalla:** Terminal del cliente en el NameNode

**[Permanecer en la misma terminal del cliente]**

> *"Además de las operaciones básicas, implementé dos funcionalidades adicionales que mejoran la observabilidad y usabilidad del sistema."*

**La primera es el comando `status`:**

```bash
python3 cli.py status
```

> *"El comando `status` consulta al NameNode mediante el nuevo RPC `GetClusterStatus` y muestra en tiempo real el estado completo del clúster: qué DataNodes están activos o caídos, cuántos bloques tiene almacenados cada uno, el espacio disponible y el timestamp del último heartbeat recibido. Esto nos permite verificar visualmente que los bloques están efectivamente distribuidos entre los tres nodos."*

**La segunda es `rmdir -r` para eliminación recursiva:**

```bash
python3 cli.py mkdir /demo_rmdir
python3 cli.py put /tmp/archivo_video.bin /demo_rmdir/archivo.bin
python3 cli.py ls /demo_rmdir
```

```bash
# Intento sin -r (debe fallar con mensaje útil)
python3 cli.py rmdir /demo_rmdir
```

> *"El sistema me protege e incluso me sugiere el comando correcto: `rmdir -r`."*

```bash
# Eliminación recursiva
python3 cli.py rmdir -r /demo_rmdir
python3 cli.py ls /
```

> *"Con la flag `-r` elimina el directorio y todo su contenido en cascada. En este caso 1 archivo fue eliminado junto con el directorio."*

---

### SEGMENTO 5 – TOLERANCIA A FALLOS (2 min)
> 📺 **Pantalla:** Terminal DataNode 2 + Terminal cliente

> *"Y ahora la prueba más importante del proyecto: ¿qué pasa si un DataNode se cae?"*

**[Ir a terminal del DataNode 2]**
```bash
docker compose down
```
> *"Acabo de detener el DataNode 2. Ya no está disponible."*

**[Esperar 5 segundos, ir a logs del NameNode]**
```bash
docker logs minidfs-namenode --tail 10
```
> *"El HeartbeatMonitor detectará en menos de 30 segundos que dn2 dejó de responder y marcará el nodo como inactivo. Vean cómo aparece el mensaje de nodo caído."*

**[Ir a terminal del cliente]**
```bash
python cli.py get /demo/archivo_demo.bin /tmp/archivo_tolerancia.bin
```
> *"A pesar de que DataNode 2 está caído, el sistema descargó el archivo exitosamente usando las réplicas en dn1 y dn3. El fallback fue completamente automático y transparente para el usuario."*

```bash
sha256sum /tmp/archivo_demo.bin /tmp/archivo_tolerancia.bin
```
> *"SHA-256 idéntico. La tolerancia a fallos funciona perfectamente en producción en AWS."*

---

### SEGMENTO 6 – CIERRE (1 min)
> 📺 **Pantalla:** Cámara frontal + mostrar GitHub brevemente

**[Mostrar el repositorio en GitHub]**

> *"Para cerrar, todo el código fuente está disponible en GitHub en el repositorio Proyecto-DFS-Sistemas-distribuidos. El proyecto incluye:"*

> *"• Un NameNode con autenticación JWT, gestión de metadatos en SQLite, y monitoreo activo de nodos."*
> *"• Tres DataNodes con almacenamiento en disco, verificación SHA-256 y replicación pipeline."*
> *"• Una CLI completa con put, get, ls, mkdir, rmdir y rm."*
> *"• Todo empaquetado en Docker y desplegado en 4 instancias EC2 reales en AWS."*

> *"Este proyecto me permitió entender en profundidad cómo funcionan sistemas como HDFS y GFS: la separación entre plano de control y plano de datos, la replicación como mecanismo de tolerancia, y el valor de la verificación de integridad en sistemas distribuidos.*
>
> *Muchas gracias."*

---

## 🎥 CONSEJOS DE GRABACIÓN

| Tip | Detalle |
|---|---|
| **Herramienta** | OBS Studio (gratis) o simplemente la grabación de pantalla de Windows (Win+G) |
| **Organiza antes** | Ten las 4 terminales SSH abiertas y ordenadas ANTES de grabar |
| **Activa el venv** | `source ~/venv-dfs/bin/activate` en la terminal del cliente |
| **Fuente grande** | Aumenta el tamaño de fuente de la terminal a 16-18pt para que se vea bien |
| **Pausa con intención** | Deja 2-3 segundos después de cada comando para que se vea la salida |
| **Habla despacio** | Habla un 20% más lento de lo normal — en cámara siempre se percibe más rápido |
| **No edites en exceso** | Una toma continua de 12 min es mejor que muchos cortes — se ve más profesional |

---

## ⏱️ CRONOGRAMA DE PANTALLAS

```
00:00 - 01:00  → Cámara frontal (introducción)
01:00 - 03:00  → README / diagrama de arquitectura
03:00 - 06:00  → VS Code (código: proto, heartbeat, blockstore, cli)
06:00 - 11:00  → Terminales SSH (demo en vivo AWS: put, get, sha256)
11:00 - 13:00  → Terminal cliente (status + rmdir -r)
13:00 - 15:00  → Terminales SSH (tolerancia a fallos)
15:00 - 16:00  → GitHub + cámara frontal (cierre)
```
