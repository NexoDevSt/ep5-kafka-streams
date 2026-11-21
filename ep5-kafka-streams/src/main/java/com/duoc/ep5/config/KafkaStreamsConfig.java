package com.duoc.ep5.config;

import com.duoc.ep5.model.MensajeDLQ;
import com.duoc.ep5.model.Venta;
import com.duoc.ep5.model.VentaAgregada;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.time.Duration;

@Configuration
public class KafkaStreamsConfig {

    private final ObjectMapper mapper = new ObjectMapper();

    // 1. Extractor de tiempo personalizado para usar eventTime
    public class MiTimestampExtractor implements org.apache.kafka.streams.processor.TimestampExtractor {
        @Override
        public long extract(org.apache.kafka.clients.consumer.ConsumerRecord<Object, Object> record, long partitionTime) {
            try {
                Venta venta = mapper.readValue(record.value().toString(), Venta.class);
                return java.time.Instant.parse(venta.eventTime()).toEpochMilli();
            } catch (Exception e) {
                return partitionTime;
            }
        }
    }

    @Bean
    public KStream<String, String> processVentas(StreamsBuilder builder) {
        // 2. Inicializamos el stream con el TimestampExtractor
        KStream<String, String> sourceStream = builder.stream(
            "ventas-raw",
            Consumed.with(Serdes.String(), Serdes.String())
                    .withTimestampExtractor(new MiTimestampExtractor())
        );

        KStream<String, String>[] branches = sourceStream.branch(
            (key, value) -> isValid(value), 
            (key, value) -> true            
        );

        // Los inválidos van al DLQ
        branches[1].mapValues((key, value) -> {
            System.err.println("❌ ERROR DE NEGOCIO O SINTAXIS. Enviando a DLQ: " + value);
            return procesarDLQ(key, value);
        }).to("ventas-raw-dlq", Produced.with(Serdes.String(), Serdes.String()));

        // Los válidos continúan el flujo
        branches[0]
            .mapValues(this::parseVenta)
            // Agrupación por Tienda con Ventana Tumbling (5 min, 2 gracia)
            .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(Venta.class)))
            .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofMinutes(5), Duration.ofMinutes(2)))
            .aggregate(
                () -> new VentaAgregada("", 0, 0.0, 0.0),
                (key, venta, acumulado) -> {
                    int conteo = acumulado.conteo() + 1;
                    double total = acumulado.montoTotal() + venta.total();
                    System.out.println("✅ PROCESANDO: " + venta.eventId() + " | Conteo actual: " + conteo);
                    return new VentaAgregada(key, conteo, total, total / conteo);
                },
                Materialized.with(Serdes.String(), new JsonSerde<>(VentaAgregada.class))
            )
            .toStream()
            .map((windowedKey, agregada) -> new KeyValue<>(windowedKey.key(), toJson(agregada)))
            .to("ventas-agregadas", Produced.with(Serdes.String(), Serdes.String()));

        return sourceStream;
    }

    private boolean isValid(String json) {
        try {
            Venta v = mapper.readValue(json, Venta.class);
            return v != null && v.total() == (v.cantidad() * v.precio());
        } catch (Exception e) { return false; }
    }

    private Venta parseVenta(String json) {
        try { return mapper.readValue(json, Venta.class); } catch (Exception e) { return null; }
    }

    private String procesarDLQ(String key, String original) {
        try { return mapper.writeValueAsString(new MensajeDLQ(original, key, "ventas-raw", "JSON inválido o cálculo incorrecto")); } 
        catch (Exception e) { return "Error"; }
    }

    private String toJson(Object obj) {
        try { return mapper.writeValueAsString(obj); } catch (Exception e) { return "{}"; }
    }
}