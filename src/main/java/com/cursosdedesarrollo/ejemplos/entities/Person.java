package com.cursosdedesarrollo.ejemplos.entities;


import lombok.AllArgsConstructor;
import lombok.Data;
import scala.math.BigInt;

@Data
@AllArgsConstructor
public class Person {
    private String name;
    private Long age;
}
