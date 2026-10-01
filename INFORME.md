Redactar un breve informe en el archivo `INFORME.md` explicando el modo en que se coordinan las instancias de Sum y Aggregation, así como el modo en el que el sistema escala respecto a los clientes, grándes volúmens de datos y la cantidad de controles.

## Mecanismos de Coordinación entre Controles

### Coordinación entre instancias de `Sum`

Las instancias de `Sum` actúan como *Mappers* y *Combiners* locales. Consumen desde una cola compartida (`inputQueue`, la cual es una Working Queue). Utilizando la implementacion anterior de TP MOM, se sabe que hay fair dispatch lo cual asegura un reparto equitativo de los datos entre las instancias.

#### Problema con el handleo del EOF
Cuando un cliente envia su señal de fin de datos `EndOfRecords`, el Gateway deposita un único mensaje EOF en `inputQueue`. Debido a la naturaleza de las working queues, RabbitMQ entrega ese mensaje a una sola de las réplicas de `Sum`. Las instancias que faltan quedarían bloqueadas esperando más datos, sin enterarse de que el cliente terminó, reteniendo en memoria sus acumulaciones locales sin consolidar.

#### Solución: Exchange de control (`sum_control`)
Para resolver esta problema sin depender de un coordinador central adicional:
Primero, se declara un exchange directo de control denominado `<SUM_PREFIX>_control`.
Despues, cada réplica de `Sum` suscribe una cola exclusiva relacionada a su identificador propio (`<SUM_PREFIX>_<ID>`).
La réplica que extrae el EOF original de `inputQueue` realiza un broadcast a través del exchange de control hacia todas las claves de ruteo de sus pares (`sum_0`, `sum_1`, etc).
Dado que la réplica emisora también recibe la notificación (o podría recibir mensajes tardíos), cada instancia mantiene un registro `clientFinished[clientID]`. Al recibir el primer EOF (sea por la cola o por el exchange de control), procesa la finalización y descarta cualquier evento siguiente para ese cliente.
Finalmente, cada réplica efectúa una limpieza local de los totales acumulados y lanza una notificación hacia la capa de Aggregation.

---

### Coordinación entre `Sum` y `Aggregation`

#### Distribución de datos: partición por hash
En lugar de difundir todas las frutas a todos los Aggregators (lo que implicaría procesamiento redundante y duplicados), `Sum` particiona los registros por hash de la fruta:
- Cada fruta se envía a una réplica de Aggregation específica.
- Los mensajes de EOF se transmiten por broadcast a todas las réplicas de Aggregation, ya que cada una necesita ser notificada de que esa réplica de `Sum` ha concluido sus envíos.

#### Barrera de sincronización en `Aggregation`
Una réplica de `Aggregation` no puede calcular su Top parcial hasta que todas las réplicas de `Sum` hayan terminado de enviarle datos para ese cliente.
1. Cada Aggregator mantiene un contador de finalización por cliente: `clientEofCount[clientID]++`.
2. Se establece una barrera con la condición:
   ```text
   clientEofCount[clientID] == SUM_AMOUNT
   ```
3. Al alcanzarse la barrera:
   Se calcula el Top local ordenando las frutas acumuladas con `FruitItem.Less()`, se elimina el estado del cliente en memoria y despues se envía el Top parcial y un mensaje EOF hacia `join_queue`.

---

### Coordinación en el `Joiner`

El `Joiner` reúne los resultados de las particiones generadas por los Aggregators:
1. Espera la recepción de `AGGREGATION_AMOUNT` EOFs para el cliente en cuestión.
2. Calcula el Top global concatenando los tops parciales recibidos y aplica un ordenamiento final para obtener los mejores K registros globales.
3. El Joiner envía exclusivamente el mensaje con el Top consolidado final a `results_queue` y saltea cualquier reenvío de EOF. El ciclo del cliente en las colas concluye limpiamente con esa única entrega.

---

## Decisiones de diseño encontradas

### Hashear por nombre de fruta vs hashear por ID de cliente

Durante el diseño del sistema se evaluaron dos alternativas para definir el criterio de particionado entre las etapas de `Sum` y `Aggregation`:

En primer lugar, la opción de **hashear por ID de cliente** (`hash(clientID) % AGGREGATION_AMOUNT`) consiste en enviar todo el flujo de datos de un cliente específico a una única instancia de Aggregation. Esta alternativa presenta la aparente ventaja de aislar las sesiones por nodo y simplificar el tráfico de control, ya que cada réplica de `Sum` solo necesitaría comunicarse con un único Aggregator por cliente. 
Sin embargo, introduce un problema de escalabilidad: si un único cliente transmite un archivo  con demasiados tipos de frutas distintas, una sola instancia de Aggregation concentraría todo el procesamiento y el consumo de memoria, mientras las restantes réplicas permanecerían completamente estaticas. Además, como ese único Aggregator procesaría la totalidad de los datos del cliente, calcularía directamente el Top final global, quitandole al nodo `Joiner` cualquier proposito real y reduciéndolo a un simple pasamanos que solo reenviaría el resultado al Gateway.

Por otro lado, la opción de **hashear por nombre de fruta** (`hash(fruit) % AGGREGATION_AMOUNT`), que fue la implementada, asigna cada variedad de fruta de manera determinística a una réplica de Aggregation específica, siguiendo el paradigma de MapReduce donde se particiona por la clave del dato. Este enfoque conlleva un paralelismo real, incluso ante un único cliente con grandes volúmenes de información, el conjunto de frutas se distribuye equitativamente entre todos los Aggregators disponibles, dividiendo la memoria y el cómputo. 
Como consecuencia de esta distribución, cada Aggregator produce un Top local sobre su subconjunto de frutas, lo que otorga al `Joiner` su rol esperado de recopilar los tops parciales de las distintas particiones y fusionarlos para obtener el Top global.

Por estas razones se optó por el particionado por nombre de fruta, ya que permite escalar ante grandes volúmenes de datos transmitidos por cliente (resolviendo una de las limitaciones del esqueleto) y preserva la idea original del pipeline y sus partes.

---

## Dimensiones de escalabilidad

### Escalabilidad ante clientes concurrentes

- Cada conexión cliente genera un `MessageHandler` con un identificador único global (`client-<timestamp>-<counter>`).
- En `Sum`, `Aggregation` y `Join`, todo el estado en memoria se estructura en maps indexados por `clientID`:
   ```go
   clientFruitItemMap map[string]map[string]fruititem.FruitItem
   ```
   Esto hace que los datos de un cliente nunca interfieran con las operaciones de otro.
- Al consumir de `results_queue`, el Gateway consulta a los handlers de clientes. Aquel cliente cuyo `clientID` coincida procesa el mensaje y responde al socket TCP correspondiente, los demás retornan `nil, nil` sin interferir.
- Una vez completada la entrega, las estructuras asociadas al cliente se eliminan de los maps.


### Escalabilidad ante grandes volúmenes de datos

- **Streaming reduction en memoria:**
   - `Sum` no almacena filas individuales en un buffer, aplica la función `Sum()` en memoria a medida que consume cada mensaje.
   - El espacio de memoria requerido en `Sum` es `O(K)`, donde `K` es el número de frutas distintas, y es independiente del número de líneas del archivo `N`.
- **Escalamiento Horizontal de la Memoria en Aggregation:**
   - Gracias al particionado por hash, cada réplica de Aggregation solo almacena en memoria un subconjunto de frutas, distribuyendo la carga.
- **Carga Constante en el Joiner:**
   - El volumen de datos que procesa el Joiner es independiente del tamaño del dataset original. Siempre procesa a lo sumo `AGGREGATION_AMOUNT * TOP_SIZE` elementos (ej. 3 * 3 = 9 frutas), resolviendo su cálculo rapidamente.

### Escalabilidad ante la multiplicidad de instancias

- **Escalabilidad de `Sum`:** Al utilizar una Working Queue en `inputQueue`, es posible aumentar la cantidad de réplicas de `Sum` en el archivo de Docker compose sin modificar el código. RabbitMQ balancea las filas entrantes entre todos los workers disponibles.
- **Escalabilidad de `Aggregation`:** El particionado `hash(fruit) % AGGREGATION_AMOUNT` permite aumentar la cantidad de réplicas de Aggregation de forma transparente a través de las varenvs.
- **Independencia de Nombres:** Ningún nombre de queue, exchange o contenedor está hardcodeado.

---

## Cambios en el middleware

En la entrega anterior (TP MOM), el canal de consumo de colas se encontraba configurado de forma fija con `prefetch_count = 1` para asegurar fair-dispatch. Pero, en un sistema pensado para procesar un flujo continuo de muchos registros livianos, este valor generaba un cuello de botella por la latencia de red, el consumidor debía esperar la ida y vuelta del paquete ACK antes de que RabbitMQ le enviara el siguiente mensaje.

Considerando el impacto en la performance, se decidió modificar este comportamiento en el middleware:
1. **Prefetch por defecto mas generoso:** Se incrementó el valor por defecto a `30`. Esta ventana permite que cada worker mantenga un buffer local de mensajes en transito mientras los ACKs se confirman asincrónicamente, sin perder la distribucion del trabajo entre las replicas.
2. **Configuracion mediante varenv:** Para dar flexibilidad en distintos entornos, el valor se lee de la varenv `PREFETCH_COUNT`. Si no se define o si el valor dado no es un entero positivo, el sistema adopta el valor `30`.
