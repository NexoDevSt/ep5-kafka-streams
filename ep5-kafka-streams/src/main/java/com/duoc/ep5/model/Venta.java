package com.duoc.ep5.model;
public record Venta(String eventId, String tiendaId, int cantidad, double precio, double total, String eventTime) {}