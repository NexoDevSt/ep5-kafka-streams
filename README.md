# DSY1107 - Desarrollo Cloud Native I
## Evaluación Parcial N° 5: Streaming resiliente de ventas

**Autores:** Matías Manríquez y Pedro Meneses  
**Asignatura:** Desarrollo Cloud Native I (DSY1107)

---

### 1. Diagrama de Arquitectura
El sistema implementa una topología de streaming tolerante a fallos. Los eventos ingresan al clúster de Kafka (soportado por ZooKeeper), son procesados por una aplicación Spring Boot con Kafka Streams, y se derivan según su validez temporal y lógica de negocio.

---

### 2. Prerrequisitos y Comandos Exactos

Para reproducir este entorno en una instancia EC2 (AWS), se utilizó Docker y Docker Compose para el clúster, y Java 21 con Maven (`mvnw` / `mvn`) para la aplicación Spring Boot y Kafka Streams.

**Levantar la infraestructura base (ajustando la IP pública de la EC2 en `KAFKA_ADVERTISED_LISTENERS`):**
```bash
docker compose down -v
docker compose up -d
```
*(Ver Captura 1 en la matriz de evidencias)*

**Creación de tópicos en el Broker 1:**
```bash
docker exec -it ep5-kafka-broker1-1 kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 2 --partitions 3 --topic ventas-raw --config min.insync.replicas=1
docker exec -it ep5-kafka-broker1-1 kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 2 --partitions 3 --topic ventas-agregadas --config min.insync.replicas=1
docker exec -it ep5-kafka-broker1-1 kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 2 --partitions 1 --topic ventas-raw-dlq --config min.insync.replicas=1
```

**Compilación, ejecución y verificación final:**
```bash
./mvnw clean test
docker exec -it ep5-kafka-broker1-1 kafka-console-consumer --bootstrap-server localhost:9092 --topic ventas-agregadas --from-beginning
docker exec -it ep5-kafka-broker1-1 kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group ventas-stream-app
```

---

### 3. Matriz de Pruebas y Evidencias

| Caso de Prueba | Descripción y Resultado Esperado | Evidencia |
| :--- | :--- | :--- |
| **Pruebas Automatizadas (Entorno)** | El proyecto compila correctamente y pasa la fase de test de Maven (`BUILD SUCCESS`) sin errores de dependencias ni fallos en el código fuente. ||
| **JSON Roto (Sintaxis DLQ)** | Se envía un payload JSON malformado (`{"eventId": "99", "tiendaId": `) a `POST /api/test?tiendaId=T01`; el stream evita descartarlo por timestamp negativo, detecta sintaxis inválida y lo deriva a `ventas-raw-dlq`. |  |
| **Deduplicación con StateStore** | Se envía dos veces el mismo evento válido (`eventId: "100"`); el procesador con `StateStore` (`deduplication-store`) registra el primero y bloquea/ignora el duplicado (`⚠️ EVENTO DUPLICADO IGNORADO`). | |
| **Evento Tardío en Ventana de Gracia** | Se envía un evento válido con marca de tiempo anterior (`eventId: "101"`), dentro de la ventana *Tumbling* de 5 minutos y su gracia de 2 minutos, siendo aceptado y acumulado en el agregado. |  |
| **Tópico Final con Ventanas** | Al consumir desde `ventas-agregadas`, se comprueba que los mensajes agregados incluyen los campos de ventana (`inicioVentana` y `finVentana`) junto con las métricas acumuladas. |  |
| **Estado del Consumer Group y Lag** | Al describir `ventas-stream-app`, se verifica que el consumidor está activo y al día (`LAG = 0` en las particiones consumidas), sin rezago pendiente tras las pruebas. | |

---

### 4. Justificación de Diseño y Límites Conocidos

-   **Configuración de Tópicos (RF y Min ISR):** Se configuró un Factor de Replicación (RF) de 2 debido a que el clúster base consta de dos brokers físicamente; un $RF=3$ arrojaría un error de asignación. Se estableció `min.insync.replicas=1` para garantizar la disponibilidad operativa: esto permite que, ante la caída del Broker 2 (como se demostró en la prueba), el productor siga inyectando datos y el consumidor siga agrupando ventanas apoyándose únicamente en el Broker 1.
-   **Límites de exactly_once_v2 vs Deduplicación de Negocio:** La configuración `exactly_once_v2` garantiza que no haya transacciones de red duplicadas a nivel de broker durante un fallo. No obstante, como se ve en la prueba de duplicados, si el productor inyecta voluntariamente el mismo evento lógico (mismo `eventId`), Kafka lo procesa de nuevo. Para deduplicar esto a nivel de negocio, se requiere implementar un almacén de estado (State Store) que filtre los IDs previamente procesados.
-   **Deduplicación con StateStore y Ventaneo con Gracia:** Se incorporó un `KeyValueStore` persistente (`deduplication-store`) mediante la Processor API para filtrar `eventId` duplicados a nivel de negocio, combinado con ventanas *Tumbling* de 5 minutos y 2 minutos de gracia (`TimeWindows.ofSizeAndGrace`) que emiten `inicioVentana` y `finVentana` en `ventas-agregadas`.
-   **Extracción de Timestamp Resiliente y Enrutamiento a DLQ:** En `MiTimestampExtractor`, ante fallos de parseo por payloads malformados, se utiliza el tiempo del sistema como respaldo para evitar que Kafka Streams descarte el mensaje por timestamp negativo antes de que `validarVenta` lo envíe a `ventas-raw-dlq`.
-   **Restauración de Estado (Offsets y Changelog):** Durante la detención de la aplicación, los mensajes inyectados son retenidos en el tópico de Kafka. Al reiniciar Spring Boot, el consumidor se reconecta exitosamente desde el último offset confirmado (commit) y procesa los eventos acumulados de forma automática, garantizando la continuidad del negocio sin pérdida silenciosa de datos.
-   **Punto Único de Falla:** En esta arquitectura base, ZooKeeper operando en un solo nodo sigue siendo un punto único de falla (SPOF) crítico.

**Anotacion**
Cambiar ip de docker compose por ip propia.
