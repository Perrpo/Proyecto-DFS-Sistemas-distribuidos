# ðŸŽ¬ GuiÃ³n â€“ Video DemostraciÃ³n MiniDFS
## DuraciÃ³n objetivo: 12â€“15 minutos | Solo presentador: AndrÃ©s NÃºÃ±ez

---

## ðŸ“‹ ESTRUCTURA GENERAL

| Segmento | Tema | Tiempo |
|---|---|---|
| 1 | IntroducciÃ³n y presentaciÃ³n | 1 min |
| 2 | Arquitectura del sistema | 2 min |
| 3 | Recorrido por el cÃ³digo | 3 min |
| 4 | Demo en vivo â€“ AWS EC2 (put, get, sha256) | 5 min |
| 4b | Demo nuevas funciones (status, rmdir -r) | 2 min |
| 5 | Tolerancia a fallos | 2 min |
| 6 | Cierre | 1 min |

---

## ðŸŽ™ï¸ GUIÃ“N COMPLETO

---

### SEGMENTO 1 â€“ INTRODUCCIÃ“N (1 min)
> ðŸ“º **Pantalla:** CÃ¡mara frontal + diapositiva con el nombre del proyecto

**[Hablar mirando a cÃ¡mara]**

> *"Hola, soy AndrÃ©s NÃºÃ±ez, estudiante de IngenierÃ­a de Sistemas de la Universidad Pontificia Bolivariana. En este video voy a presentar MiniDFS, un Sistema de Archivos Distribuidos por Bloques que desarrollÃ© para la materia de Arquitecturas de Nube y Sistemas Distribuidos.*
>
> *MiniDFS es una implementaciÃ³n minimalista inspirada en HDFS, el sistema de archivos de Hadoop, y en GFS, el sistema de archivos de Google. El objetivo es almacenar archivos grandes de forma distribuida entre mÃºltiples nodos, con replicaciÃ³n automÃ¡tica y tolerancia a fallos.*
>
> *Durante esta demostraciÃ³n voy a mostrarles la arquitectura del sistema, el cÃ³digo implementado, y una ejecuciÃ³n en vivo sobre cuatro instancias de AWS EC2 donde el sistema estÃ¡ corriendo en producciÃ³n."*

---

### SEGMENTO 2 â€“ ARQUITECTURA (2 min)
> ðŸ“º **Pantalla:** Compartir pantalla â†’ abrir el README.md o el informe Word con los diagramas

**[SeÃ±alar el diagrama de arquitectura]**

> *"La arquitectura sigue el patrÃ³n Maestroâ€“Trabajadores. Tenemos tres componentes principales:*
>
> *Primero, el **NameNode**: es el cerebro del sistema. Gestiona todos los metadatos, la autenticaciÃ³n de usuarios con tokens JWT, el espacio de nombres de archivos y coordina dÃ³nde se almacena cada bloque. Corre en el puerto 50051.*
>
> *Segundo, los **DataNodes**: son los trabajadores de almacenamiento. En nuestro caso tenemos tres: dn1, dn2 y dn3, corriendo en los puertos 50061, 50062 y 50063. Cada uno almacena bloques en disco con verificaciÃ³n de integridad SHA-256.*
>
> *Tercero, el **Cliente**: una interfaz de lÃ­nea de comandos con la que el usuario sube, descarga y gestiona sus archivos.*
>
> *La comunicaciÃ³n entre todos los componentes se hace mediante **gRPC con Protocol Buffers**, que es mucho mÃ¡s eficiente que REST para transferencia de datos en streaming.*
>
> *[SeÃ±alar flecha de replicaciÃ³n pipeline]*
>
> *Un punto clave del diseÃ±o es la **replicaciÃ³n pipeline**: cuando el cliente sube un bloque, lo envÃ­a solo al primer DataNode. Ese nodo lo reenvÃ­a al segundo, y el segundo al tercero. AsÃ­ el cliente solo usa su ancho de banda una vez, y los DataNodes se encargan de la replicaciÃ³n entre sÃ­, igual que hace HDFS."*

---

### SEGMENTO 3 â€“ RECORRIDO POR EL CÃ“DIGO (3 min)
> ðŸ“º **Pantalla:** VS Code con el proyecto abierto

**[Abrir VS Code, mostrar la estructura de carpetas]**

> *"Veamos rÃ¡pidamente la estructura del proyecto. Tenemos cuatro carpetas principales.*
>
> *En **proto/** estÃ¡ el contrato gRPC, el archivo dfs.proto que define todos los servicios y mensajes del sistema. Este es el punto de verdad de toda la comunicaciÃ³n."*

**[Abrir proto/dfs.proto]**

> *"AquÃ­ podemos ver por ejemplo el servicio StoreBlock, que es client-streaming, lo que significa que el cliente envÃ­a el bloque en chunks de 4 megabytes. Y RetrieveBlock es server-streaming, el DataNode le envÃ­a el bloque al cliente en chunks de 1 megabyte."*

**[Abrir namenode/heartbeat_monitor.py]**

> *"En el NameNode, el componente mÃ¡s interesante es el HeartbeatMonitor. Es un thread daemon que corre en segundo plano y monitorea si los DataNodes siguen vivos. Cada DataNode envÃ­a un heartbeat cada 10 segundos. Si pasan mÃ¡s de 30 segundos sin heartbeat, el NameNode marca ese nodo como inactivo y automÃ¡ticamente busca los bloques que quedaron sub-replicados para replicarlos en otros nodos disponibles. Esto es lo que nos da la tolerancia a fallos."*

**[Abrir datanode/block_store.py]**

> *"En el DataNode, el BlockStore es donde se almacenan fÃ­sicamente los bloques en disco. Cada vez que se escribe un bloque, se calcula su SHA-256 y se guarda junto al bloque. Al momento de leer, se vuelve a calcular y se compara. Si no coinciden, el bloque estÃ¡ corrupto y el sistema automÃ¡ticamente busca una rÃ©plica en otro nodo."*

**[Abrir client/cli.py]**

> *"Y finalmente el cliente. La CLI implementa todos los comandos requeridos: put, get, ls, mkdir, rmdir, rm, register y login. La autenticaciÃ³n usa JWT, por lo que el token se persiste localmente y se envÃ­a en cada solicitud al NameNode."*

---

### SEGMENTO 4 â€“ DEMO EN VIVO AWS EC2 (5 min)
> ðŸ“º **Pantalla:** 4 terminales SSH abiertas + PowerShell local

**[Mostrar las 4 terminales, cada una conectada a una instancia EC2]**

> *"Bien, vamos a la parte mÃ¡s importante: la demostraciÃ³n en vivo. Tengo cuatro instancias de AWS EC2 corriendo en us-east-1. En esta terminal estÃ¡ el NameNode con IP privada 172.31.0.165. Y en estas tres estÃ¡n los DataNodes."*

**[Ir a terminal del NameNode]**

```bash
docker ps
```
> *"Vemos que el contenedor minidfs-namenode estÃ¡ corriendo y en estado healthy. Puerto 50051 expuesto."*

**[Ir a terminal DataNode 1]**
```bash
docker ps
docker logs minidfs-datanode1 --tail 5
```
> *"DataNode 1 estÃ¡ conectado al NameNode en 172.31.0.165 y enviando BlockReports periÃ³dicamente."*

**[Ir a terminal del NameNode, ver logs]**
```bash
docker logs minidfs-namenode --tail 15
```
> *"AquÃ­ en los logs del NameNode vemos los BlockReports que llegan de dn1, dn2 y dn3. El clÃºster estÃ¡ completamente operativo."*

---

**[Ir a la terminal con el cliente activo (venv activado)]**

```bash
cd ~/Proyecto-DFS-Sistemas-distribuidos/client
source ~/venv-dfs/bin/activate
```

> *"Ahora vamos a usar el cliente. Primero registro un usuario y me autentico:"*

```bash
python3 cli.py register andres pass123
python3 cli.py login andres pass123
```
> *"Usuario registrado y autenticado. El NameNode generÃ³ un token JWT que el cliente guarda localmente."*

```bash
python3 cli.py ls /
```
> *"El sistema de archivos estÃ¡ vacÃ­o. Creemos un directorio:"*

```bash
python3 cli.py mkdir /demo
python3 cli.py ls /
```

---

> *"Ahora viene la parte mÃ¡s interesante: subir un archivo. Voy a crear un archivo de prueba de 10 megabytes con datos aleatorios:"*

```bash
dd if=/dev/urandom of=/tmp/archivo_demo.bin bs=1M count=10
ls -lh /tmp/archivo_demo.bin
```

```bash
python3 cli.py put /tmp/archivo_demo.bin /demo/archivo_demo.bin
```

> *"Observen la salida: el archivo fue dividido en bloques y el bloque fue enviado a un DataNode primario. Vemos el pipeline de replicaciÃ³n: el bloque viajÃ³ de dn1 a dn2 a dn3 automÃ¡ticamente. 1 de 1 bloques confirmados."*

```bash
python3 cli.py ls /demo
```
> *"Podemos ver el archivo en el DFS con su tamaÃ±o correcto."*

---

> *"Ahora lo descargamos:"*

```bash
python3 cli.py get /demo/archivo_demo.bin /tmp/archivo_descargado.bin
```

> *"Descarga exitosa. Y lo mÃ¡s importante: verificamos la integridad con SHA-256:"*

```bash
sha256sum /tmp/archivo_demo.bin /tmp/archivo_descargado.bin
```

> *"Los dos hashes son idÃ©nticos. El archivo que descargamos es exactamente igual al que subimos, byte por byte. La integridad estÃ¡ garantizada."*

---

### SEGMENTO 4b â€“ NUEVAS FUNCIONALIDADES (2 min)
> ðŸ“º **Pantalla:** Terminal del cliente en el NameNode

**[Permanecer en la misma terminal del cliente]**

> *"AdemÃ¡s de las operaciones bÃ¡sicas, implementÃ© dos funcionalidades adicionales que mejoran la observabilidad y usabilidad del sistema."*

**La primera es el comando `status`:**

```bash
python3 cli.py status
```

> *"El comando `status` consulta al NameNode mediante el nuevo RPC `GetClusterStatus` y muestra en tiempo real el estado completo del clÃºster: quÃ© DataNodes estÃ¡n activos o caÃ­dos, cuÃ¡ntos bloques tiene almacenados cada uno, el espacio disponible y el timestamp del Ãºltimo heartbeat recibido. Esto nos permite verificar visualmente que los bloques estÃ¡n efectivamente distribuidos entre los tres nodos."*

**La segunda es `rmdir -r` para eliminaciÃ³n recursiva:**

```bash
python3 cli.py mkdir /demo_rmdir
python3 cli.py put /tmp/archivo_video.bin /demo_rmdir/archivo.bin
python3 cli.py ls /demo_rmdir
```

```bash
# Intento sin -r (debe fallar con mensaje Ãºtil)
python3 cli.py rmdir /demo_rmdir
```

> *"El sistema me protege e incluso me sugiere el comando correcto: `rmdir -r`."*

```bash
# EliminaciÃ³n recursiva
python3 cli.py rmdir -r /demo_rmdir
python3 cli.py ls /
```

> *"Con la flag `-r` elimina el directorio y todo su contenido en cascada. En este caso 1 archivo fue eliminado junto con el directorio."*

---

### SEGMENTO 5 â€“ TOLERANCIA A FALLOS (2 min)
> ðŸ“º **Pantalla:** Terminal DataNode 2 + Terminal cliente

> *"Y ahora la prueba mÃ¡s importante del proyecto: Â¿quÃ© pasa si un DataNode se cae?"*

**[Ir a terminal del DataNode 2]**
```bash
docker compose down
```
> *"Acabo de detener el DataNode 2. Ya no estÃ¡ disponible."*

**[Esperar 5 segundos, ir a logs del NameNode]**
```bash
docker logs minidfs-namenode --tail 10
```
> *"El HeartbeatMonitor detectarÃ¡ en menos de 30 segundos que dn2 dejÃ³ de responder y marcarÃ¡ el nodo como inactivo. Vean cÃ³mo aparece el mensaje de nodo caÃ­do."*

**[Ir a terminal del cliente]**
```bash
python3 cli.py get /demo/archivo_demo.bin /tmp/archivo_tolerancia.bin
```
> *"A pesar de que DataNode 2 estÃ¡ caÃ­do, el sistema descargÃ³ el archivo exitosamente usando las rÃ©plicas en dn1 y dn3. El fallback fue completamente automÃ¡tico y transparente para el usuario."*

```bash
sha256sum /tmp/archivo_demo.bin /tmp/archivo_tolerancia.bin
```
> *"SHA-256 idÃ©ntico. La tolerancia a fallos funciona perfectamente en producciÃ³n en AWS."*

---

### SEGMENTO 6 â€“ CIERRE (1 min)
> ðŸ“º **Pantalla:** CÃ¡mara frontal + mostrar GitHub brevemente

**[Mostrar el repositorio en GitHub]**

> *"Para cerrar, todo el cÃ³digo fuente estÃ¡ disponible en GitHub en el repositorio Proyecto-DFS-Sistemas-distribuidos. El proyecto incluye:"*

> *"â€¢ Un NameNode con autenticaciÃ³n JWT, gestiÃ³n de metadatos en SQLite, y monitoreo activo de nodos."*
> *"â€¢ Tres DataNodes con almacenamiento en disco, verificaciÃ³n SHA-256 y replicaciÃ³n pipeline."*
> *"â€¢ Una CLI completa con put, get, ls, mkdir, rmdir y rm."*
> *"â€¢ Todo empaquetado en Docker y desplegado en 4 instancias EC2 reales en AWS."*

> *"Este proyecto me permitiÃ³ entender en profundidad cÃ³mo funcionan sistemas como HDFS y GFS: la separaciÃ³n entre plano de control y plano de datos, la replicaciÃ³n como mecanismo de tolerancia, y el valor de la verificaciÃ³n de integridad en sistemas distribuidos.*
>
> *Muchas gracias."*

---

## ðŸŽ¥ CONSEJOS DE GRABACIÃ“N

| Tip | Detalle |
|---|---|
| **Herramienta** | OBS Studio (gratis) o simplemente la grabaciÃ³n de pantalla de Windows (Win+G) |
| **Organiza antes** | Ten las 4 terminales SSH abiertas y ordenadas ANTES de grabar |
| **Activa el venv** | `source ~/venv-dfs/bin/activate` en la terminal del cliente |
| **Fuente grande** | Aumenta el tamaÃ±o de fuente de la terminal a 16-18pt para que se vea bien |
| **Pausa con intenciÃ³n** | Deja 2-3 segundos despuÃ©s de cada comando para que se vea la salida |
| **Habla despacio** | Habla un 20% mÃ¡s lento de lo normal â€” en cÃ¡mara siempre se percibe mÃ¡s rÃ¡pido |
| **No edites en exceso** | Una toma continua de 12 min es mejor que muchos cortes â€” se ve mÃ¡s profesional |

---

## â±ï¸ CRONOGRAMA DE PANTALLAS

```
00:00 - 01:00  â†’ CÃ¡mara frontal (introducciÃ³n)
01:00 - 03:00  â†’ README / diagrama de arquitectura
03:00 - 06:00  â†’ VS Code (cÃ³digo: proto, heartbeat, blockstore, cli)
06:00 - 11:00  â†’ Terminales SSH (demo en vivo AWS: put, get, sha256)
11:00 - 13:00  â†’ Terminal cliente (status + rmdir -r)
13:00 - 15:00  â†’ Terminales SSH (tolerancia a fallos)
15:00 - 16:00  â†’ GitHub + cÃ¡mara frontal (cierre)
```
