# TP COORDINACION
### ALUMNO
* **Nombre y Apellido:** Julian Mutchinick
* **Mail**: jmutchinick@fi.uba.ar
* **Padron**: 99479

## GENERAL
### Protocolo interno: Envelope
Toda la comunicacion entre los nodos (gateway, sum, aggregation y join) a traves del middleware se hace con un unico formato de mensaje: el **Envelope**.

El Envelope es un "sobre" que envuelve a los `FruitItem` junto con la metadata necesaria para coordinar. Se serializa en **JSON**.

```go
type Envelope struct {
	ClientId string                `json:"client_id"`
	Type     string                `json:"type"`
	Data     []fruititem.FruitItem `json:"data"`
}
```

**ClientId:** identifica a que cliente pertenece el mensaje. Es un UUID generado por el `MessageHandler` del gateway (uno por cada conexion) y se propaga por todo el pipeline.

**Type:** tipo de mensaje (ver abajo)).

**Data:** los `FruitItem`. `FruitItem` se trata como opaco: solo se usan sus funciones `Sum` y `Less`.

Obs.: `FruitItem` no se modifica (lo reemplazan al corregir). Por eso el client id no puede ir dentro del item y va en el sobre.

La construccion de los envelopes esta centralizada en el paquete `inner` (`NewDataEnvelope`, `NewEofEnvelope`, `NewBroadcastEofEnvelope`). Los nodos solo deciden *que* mandar, no *como* se arma el mensaje.

---

**existen los siguientes tipos de envelope:**

***-DATA:***

proposito: transporta frutas. Segun donde viaje, significa algo distinto:
- gateway -> sum: un registro del cliente.
- sum -> aggregation: los totales parciales de un cliente para las frutas que le corresponden a ese aggregation.
- aggregation -> join: el top parcial de un cliente.
- join -> gateway: el top final de un cliente.

payload: lista de `FruitItem`.

***-EOF:***

proposito: indica que el emisor ya mando todo lo de un cliente.
- gateway -> sum: el cliente termino de enviar sus registros.
- sum -> aggregation: ese sum ya volco todo lo de ese cliente.
- aggregation -> join: ese aggregation ya mando su top parcial de ese cliente.

no tiene payload.

Obs.: el join **no** manda EOF al gateway. El gateway espera exactamente un mensaje por cliente (el top), asi que cualquier mensaje extra nadie lo reclamaria.

***-BROADCAST_EOF:***

proposito: lo usa un sum para avisarle a **todos** los sum (incluido el mismo) que un cliente termino, para que cada uno vuelque sus parciales de ese cliente.

no tiene payload.

---
## MULTIPLES CLIENTES
Cada cliente es una "consulta" independiente que atraviesa el mismo pipeline al mismo tiempo que las demas.

- El `MessageHandler` (uno por conexion) genera un UUID al crearse y lo pone en cada mensaje que serializa.
- Todos los nodos guardan su estado **por cliente**: `map[clientId]map[fruta]FruitItem` en sum y aggregation, y `map[clientId][]FruitItem` en join. Los contadores de EOF tambien son por cliente.
- Cuando un nodo termina con un cliente, **libera su estado** (`delete`), asi la memoria no crece con la cantidad de clientes atendidos.
- A la vuelta, el gateway le pregunta a cada handler si el resultado es suyo. El handler compara el `ClientId` del envelope con el suyo y devuelve `nil` si no le pertenece, para que el gateway pruebe con el siguiente.

---

## SUM
### Distribucion del trabajo
Todos los sum consumen de la **misma cola** (`input_queue`). Es una work queue: RabbitMQ le entrega cada mensaje a un unico sum. Cada sum acumula por cliente y por fruta usando `FruitItem.Sum`.

### Coordinacion del EOF entre sum
Problema: el EOF de un cliente tambien viaja por la work queue, asi que le llega a **un solo** sum. Los demas tienen parciales de ese cliente y no se enterarian.

Solucion:
- Cada sum tiene un exchange de nodo (nombre `SUM_PREFIX`). Cada sum consume con su propia routing key (`SUM_PREFIX_ID`).
- El sum que recibe el EOF del gateway **no envia directamente**: publica un `BROADCAST_EOF` con las routing keys de todos los sum, incluido el mismo.
- Cada sum, al recibir el `BROADCAST_EOF`, envia sus parciales de ese cliente hacia los aggregation y manda su EOF.

Asi los N sum se comportan igual, por el mismo camino, y cada uno manda exactamente un EOF por cliente.

##### **nota:** Se uso un exchange y no una cola porque el aviso tiene que llegarle a **todos** los sum. Una cola compartida tendria el mismo problema que el EOF original.

### Concurrencia dentro del sum
El sum consume de dos lugares (datos del gateway y avisos de los otros sum), cada uno en su propia go routine.

Ambos handlers toman un **Mutex** al entrar.

### Carrera entre datos y aviso de EOF
Los datos y el aviso llegan por colas distintas, y entre colas distintas no hay orden garantizado. Podria pasar que un sum reciba el aviso de un cliente cuando todavia tiene un mensaje de ese cliente sin procesar.

Como se resuelve:
- **FIFO en `input_queue`:** el gateway publica todos los DATA de un cliente y despues su EOF. Cuando un sum saca el EOF, ningun DATA de ese cliente queda en la cola: solo pueden estar en procesamiento en algun sum.
- **prefetch = 1**: cada sum tiene como maximo un mensaje entregado sin ack.
- **ack despues de procesar:** RabbitMQ no le manda el siguiente mensaje a un sum hasta que termina el actual.
- **Mutex:** si llega el aviso mientras se procesa un dato, no se envia al aggrator, sino que espera a que ese dato termine, y lo incluye.
---

## AGGREGATION
### Particionado por fruta
Cada sum le mandaba todo a todos los aggregation, ahora cada fruta va siempre al mismo aggregation:

```
aggregation = fnv32a(nombre de la fruta) % AGGREGATION_AMOUNT
```

- Es deterministico entre procesos: todos los sum eligen el mismo aggregation para la misma fruta.
- Se usa solo el nombre de la fruta.
- El sum tiene un middleware de salida por aggregation, cada uno con una sola routing key (`AGGREGATION_PREFIX_i`), para poder mandarle a uno puntual.

Al enviar un cliente, el sum:
- agrupa sus frutas por aggregation y le manda a cada uno **un solo DATA** con sus frutas (si tiene alguna).
- le manda el **EOF a todos** los aggregation, tengan frutas o no. Si no, el que no recibio frutas nunca completaria su cuenta.

### Coordinacion del EOF.
Cada aggregation cuenta los EOF por cliente. Cuando llega a `SUM_AMOUNT`, todos los sum ya le mandaron todo lo de ese cliente (cada sum manda sus DATA antes que su EOF por el mismo canal, y RabbitMQ mantiene ese orden). Recien ahi arma el top parcial.

---

## JOIN
- Junta los tops parciales por cliente.
- Cuenta los EOF por cliente hasta `AGGREGATION_AMOUNT`.
- Ordena con `Less`, corta en `TOP_SIZE` y manda **un solo DATA** al gateway, aunque el top este vacio (si no, ese cliente quedaria esperando).

---
## ESCALABILIDAD
### Respecto a los clientes
- Cada cliente tiene su UUID y su estado separado en cada nodo, asi que se atienden en paralelo por el mismo pipeline sin mezclarse.
- Un cliente puede recibir su resultado mientras otros todavia estan enviando datos.
- El estado de cada cliente se libera cuando termina, asi la memoria no crece con la cantidad de clientes atendidos.

### Respecto al volumen de datos
Los datos se van "recortando" a medida que avanzan por el pipeline. Cada nodo le pasa al siguiente bastante menos de lo que recibio:
- **sum:** recibe un mensaje por cada registro del cliente, pero manda a lo sumo un mensaje por aggregation, con un total por fruta. Por mas que un cliente mande millones de registros de manzana, sale un solo `("manzana", total)`.
- **aggregation:** recibe los totales de todos los sum, pero manda solo su top parcial (`TOP_SIZE` frutas), no todos sus totales.
- **join:** recibe un top parcial por aggregation y manda un solo top final.

Ademas, con prefetch = 1 el trabajo se reparte parejo entre los sum: ninguno acapara mensajes mientras otro esta libre.

### Respecto a la cantidad de nodos
- Todo sale de la configuracion (`SUM_AMOUNT`, `AGGREGATION_AMOUNT`, `SUM_PREFIX`, `AGGREGATION_PREFIX`, `ID`). No hay nombres ni cantidades fijas en el codigo.
- **sum:** agregar instancias reparte mas el trabajo de la work queue. El costo de coordinacion es un broadcast por cliente.
- **aggregation:** agregar instancias reparte las frutas. Cada fruta la procesa un unico aggregation, asi que no hay trabajo repetido.
- **join:** es una sola instancia, pero recibe poco volumen (`AGGREGATION_AMOUNT` tops de `TOP_SIZE` frutas por cliente).
##### **nota:** se podria "jugar" con estos parametros si el volumen de clientes y datos escala para buscar una buena configuracion (buscar cuellos de botella, etc)

---
## GRACEFUL SHUTDOWN
Sum, aggregation y join siguen el mismo esquema:

- Los consumidores corren en go routines.
- El hilo principal queda bloqueado esperando SIGTERM (`common.HandleSignals`).
- Al llegar la señal se llama a `StopConsuming`, que cancela el consumer de RabbitMQ. El canal de deliveries se cierra y `StartConsuming` retorna.
- Un **WaitGroup** espera a que todos los consumidores terminen. Asi no se cierra una conexion que otra go routine todavia usa.
- Se cierran todos los middlewares y el proceso termina con codigo 0.

##### **nota:** si llega la señal mientras se procesa un mensaje, ese mensaje se termina de procesar (con su ack) antes de que `StartConsuming` retorne.
---
## COMMON PACKAGE
Varios nodos repetian la misma logica (armar el top, crear las routing keys, cerrar los middlewares, esperar el SIGTERM, etc). Para no tener el mismo codigo copiado en sum, aggregation y join, lo movi al package `common`.

Lo separe en dos partes:

***-helpers.go:***

funciones que tienen logica del negocio, o sea que saben de frutas, aggregators o de como funciona este sistema:
- `TopFruits`: ordena las frutas usando `Less` y corta en el tamaño del top. La usan aggregation y join.
- `BuildExchangeRouteKeys`: arma las routing keys (`prefix_0`, `prefix_1`, ...) a partir del prefijo y la cantidad de nodos.
- `SplitFruitsByAggregator`: reparte las frutas de un cliente entre los aggregation (usa el hash fnv del nombre de la fruta).
- `CloseMiddlewares`: cierra una lista de middlewares.
- `HandleSignals`: se queda bloqueado hasta que llega un SIGTERM. La usan los tres nodos para el graceful shutdown.

***-tkt (toolkit):***

funciones genericas, sin logica de negocio. No saben nada del TP y se podrian usar en cualquier otro  proyecto (de hecho las copie de los TPs previos):
- `FormatError`: arma un error a partir de un mensaje.
- `PanicOnErr`: hace panic si hay un error.
