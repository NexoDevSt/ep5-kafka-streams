package com.duoc.ep5.controller;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api")
public class PruebasController {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public PruebasController(KafkaTemplate<String, String> kafkaTemplate) { 
        this.kafkaTemplate = kafkaTemplate; 
    }

    @PostMapping("/test")
    public String enviarEvento(@RequestBody String json, @RequestParam String tiendaId) {
        // La clave es tiendaId para garantizar la partición correcta
        kafkaTemplate.send("ventas-raw", tiendaId, json);
        return "Evento inyectado correctamente en ventas-raw para la tienda: " + tiendaId;
    }
}
