package com.cursosdedesarrollo.ejemplos.entities;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Employee3 {
    private String department;
    private Integer age;
    private Integer salary;
    private Boolean active;
}
