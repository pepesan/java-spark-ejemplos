package com.cursosdedesarrollo.ejemplos.pasoparametros;

public class Main {
    public static void main(String[] args) {
        pasaParametros("hola");
        pasaParametros("hola", "adios", "hola", "adios");
    }

    public static void pasaParametros(String... datos){
        for (int i = 0 ; i< datos.length; i++){
            System.out.println(datos[i]);
        }
    }
}
