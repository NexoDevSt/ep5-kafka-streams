package com.duoc.ep5.config;

import com.duoc.ep5.model.MensajeDLQ;
import com.duoc.ep5.model.Venta;
import com.duoc.ep5.model.VentaAgregada;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.time.Duration;
import java.time.Instant;

@Configuration
public class KafkaStreamsConfig {

    private final ObjectMapper mapper = new ObjectMapper();

    public class MiTimestampExtractor implements org.apache.kafka.streams.processor.TimestampExtractor {
        @Override
        public long extract(org.apache.kafka.clients.consumer.ConsumerRecord<Object, Object> record, long partitionTime) {
            try {
                Venta venta = mapper.readValue(record.value().toString(), Venta.class);
                return Instant.parse(venta.eventTime()).toEpochMilli();
            } catch (Exception e) { 
                // SOLUCIÓN: En vez de devolver un número negativo que hace que Kafka ignore el mensaje,
                // devolvemos el tiempo actual para que pase directo al validador y luego al DLQ.
                return System.currentTimeMillis(); 
            }
        }
    }

    @Bean
    public KStream<String, String> processVentas(StreamsBuilder builder) {
        StoreBuilder<KeyValueStore<String, Long>> deduplicationStore = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("deduplication-store"),
                Serdes.String(), Serdes.Long()
        );
        builder.addStateStore(deduplicationStore);

        KStream<String, String> sourceStream = builder.stream(
                "ventas-raw",
                Consumed.with(Serdes.String(), Serdes.String()).withTimestampExtractor(new MiTimestampExtractor())
        );

        KStream<String, String>[] branches = sourceStream.branch(
                (key, value) -> validarVenta(value).equals("OK"),
                (key, value) -> true
        );

        branches[1].mapValues((key, value) -> {
            String motivo = validarVenta(value);
            System.err.println("❌ ERROR: " + motivo + " -> DLQ: " + value);
            return procesarDLQ(key, value, motivo);
        }).to("ventas-raw-dlq", Produced.with(Serdes.String(), Serdes.String()));

        branches[0]
                .mapValues(this::parseVenta)
                .process(() -> new Processor<String, Venta, String, Venta>() {
                    private KeyValueStore<String, Long> stateStore;
                    private ProcessorContext<String, Venta> context;

                    @Override
                    public void init(ProcessorContext<String, Venta> context) {
                        this.context = context;
                        this.stateStore = context.getStateStore("deduplication-store");
                    }

                    @Override
                    public void process(Record<String, Venta> record) {
                        Venta v = record.value();
                        if (stateStore.get(v.eventId()) != null) {
                            System.err.println("⚠️ EVENTO DUPLICADO IGNORADO: eventId=" + v.eventId());
                        } else {
                            stateStore.put(v.eventId(), System.currentTimeMillis());
                            this.context.forward(record);
                        }
                    }
                }, "deduplication-store")
                .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(Venta.class)))
                .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofMinutes(5), Duration.ofMinutes(2)))
                .aggregate(
                        () -> new VentaAgregada("", 0, 0.0, 0.0, "", ""),
                        (key, venta, acumulado) -> {
                            int conteo = acumulado.conteo() + 1;
                            double total = acumulado.montoTotal() + venta.total();
                            System.out.println("✅ PROCESANDO: " + venta.eventId() + " | Conteo: " + conteo);
                            return new VentaAgregada(key, conteo, total, total / conteo, "", "");
                        },
                        Materialized.with(Serdes.String(), new JsonSerde<>(VentaAgregada.class))
                )
                .toStream()
                .map((windowedKey, agregada) -> {
                    String inicio = Instant.ofEpochMilli(windowedKey.window().start()).toString();
                    String fin = Instant.ofEpochMilli(windowedKey.window().end()).toString();
                    VentaAgregada finalAgregada = new VentaAgregada(
                            windowedKey.key(), agregada.conteo(), agregada.montoTotal(),
                            agregada.ticketPromedio(), inicio, fin
                    );
                    return new KeyValue<>(windowedKey.key(), toJson(finalAgregada));
                })
                .to("ventas-agregadas", Produced.with(Serdes.String(), Serdes.String()));

        return sourceStream;
    }

    private String validarVenta(String json) {
        try {
            JsonNode node = mapper.readTree(json);
            if (!node.has("eventId") || !node.has("tiendaId") || !node.has("cantidad") || !node.has("precio") || !node.has("total") || !node.has("eventTime")) {
                return "Campos obligatorios ausentes";
            }
            Venta v = mapper.treeToValue(node, Venta.class);
            if (v.total() != (v.cantidad() * v.precio())) return "Regla de negocio: Total incorrecto";
            return "OK";
        } catch (Exception e) { return "Sintaxis JSON malformada"; }
    }

    private Venta parseVenta(String json) {
        try { return mapper.readValue(json, Venta.class); } catch (Exception e) { return null; }
    }

    private String procesarDLQ(String key, String original, String motivo) {
        try { return mapper.writeValueAsString(new MensajeDLQ(original, key, "ventas-raw", motivo)); } catch (Exception e) { return "Error"; }
    }

    private String toJson(Object obj) {
        try { return mapper.writeValueAsString(obj); } catch (Exception e) { return "{}"; }
    }
}