package br.mackenzie.biblioteca.server;

import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;

public class ServidorBiblioteca {
    public static void main(String[] args) throws IOException, InterruptedException {
        int port = 50051;
        Server server = ServerBuilder.forPort(port)
                .addService(new BibliotecaServiceImpl())
                .build()
                .start();

        System.out.println("Servidor Biblioteca gRPC iniciado na porta " + port);
        server.awaitTermination();
    }
}