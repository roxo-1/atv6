package br.mackenzie.biblioteca.server;

import br.mackenzie.biblioteca.model.Constantes;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptors;

import java.io.IOException;

public class ServidorBiblioteca {

    public static void main(String[] args) throws IOException, InterruptedException {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : Constantes.PORTA_PADRAO;

        // O ÚLTIMO interceptor da lista é executado primeiro:
        // o log vê todas as chamadas (inclusive as rejeitadas) e depois vem a autenticação.
        Server server = ServerBuilder.forPort(port)
                .addService(ServerInterceptors.intercept(
                        new BibliotecaServiceImpl(),
                        new AuthInterceptor(),
                        new LogInterceptor()))
                .build()
                .start();

        System.out.println("Servidor Biblioteca gRPC iniciado na porta " + port);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Encerrando servidor...");
            server.shutdown();
        }));

        server.awaitTermination();
    }
}
