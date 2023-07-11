package com.cursosdedesarrollo.ejemplos.entities;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Employee {
    private String department;
    private String name;
    private Integer salary;
}
