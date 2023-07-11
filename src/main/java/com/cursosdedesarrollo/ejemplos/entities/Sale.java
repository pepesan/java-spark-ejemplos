package com.cursosdedesarrollo.ejemplos.entities;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Sale {
    private String Date;
    private String product;
    private Integer revenue;
}
