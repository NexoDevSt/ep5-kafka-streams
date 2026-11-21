# DSY1107 - Desarrollo Cloud Native I
## Evaluación Parcial N° 5: Streaming resiliente de ventas

**Autores:** Matías Manríquez y Pedro Meneses  
**Asignatura:** Desarrollo Cloud Native I (DSY1107)

---

### 1. Diagrama de Arquitectura
El sistema implementa una topología de streaming tolerante a fallos. Los eventos ingresan al clúster de Kafka (soportado por ZooKeeper), son procesados por una aplicación Spring Boot con Kafka Streams, y se derivan según su validez temporal y lógica de negocio.



---

### 2. Prerrequisitos y Comandos Exactos

Para reproducir este entorno en una instancia EC2 (AWS), se utilizó Docker y Docker Compose para el clúster, y Maven/Java 17 para el consumidor de Streams.

**Levantar la infraestructura base:**
```bash
docker compose up -d
```
*(Ver Captura 1 en la matriz de evidencias)*

**Creación de tópicos en el Broker 1:**
```bash
docker exec -it ep5-kafka-broker1-1 bash

kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 2 --partitions 3 --topic ventas-raw --config min.insync.replicas=1
kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 2 --partitions 3 --topic ventas-agregadas --config min.insync.replicas=1
kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 2 --partitions 1 --topic ventas-raw-dlq --config min.insync.replicas=1
```

---

### 3. Matriz de Pruebas y Evidencias

**Ver en matriz de evidencias/informe**

### 4. Justificación de Diseño y Límites Conocidos

*   **Configuración de Tópicos (RF y Min ISR):** Se configuró un Factor de Replicación (RF) de 2 debido a que el clúster base consta de dos brokers físicamente; un $RF=3$ arrojaría un error de asignación. Se estableció `min.insync.replicas=1` para garantizar la disponibilidad operativa: esto permite que, ante la caída del Broker 2 (como se demostró en la prueba), el productor siga inyectando datos y el consumidor siga agrupando ventanas apoyándose únicamente en el Broker 1.
*   **Límites de exactly_once_v2 vs Deduplicación:** La configuración `exactly_once_v2` previene transacciones de red duplicadas a nivel de broker. No obstante, si el productor envía el mismo evento de negocio repetido, Kafka lo procesa como un mensaje nuevo.
*   **Punto Único de Falla:** En esta arquitectura base, ZooKeeper operando en un solo nodo sigue siendo un punto único de falla (SPOF) crítico.


**Anotacion**
Cambiar ip de docker compose por ip propia.