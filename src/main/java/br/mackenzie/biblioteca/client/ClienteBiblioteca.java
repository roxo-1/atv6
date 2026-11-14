package br.mackenzie.biblioteca.client;

import br.mackenzie.biblioteca.grpc.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ClienteBiblioteca {
    public static void main(String[] args) throws InterruptedException {
        String target = "localhost:50051";
        ManagedChannel channel = ManagedChannelBuilder.forTarget(target)
                .usePlaintext()
                .build();

        BibliotecaServiceGrpc.BibliotecaServiceBlockingStub blockingStub = BibliotecaServiceGrpc.newBlockingStub(channel);
        BibliotecaServiceGrpc.BibliotecaServiceStub asyncStub = BibliotecaServiceGrpc.newStub(channel);

        try {
            System.out.println("--- 1️⃣ TESTE: Cadastrar 3 livros (Unary) ---");
            String[] isbns = {"978-85-7522", "978-01-3468", "978-85-5080"};
            String[] titulos = {"Clean Code", "Effective Java", "Arquitetura Limpa"};
            
            for (int i = 0; i < 3; i++) {
                try {
                    CadastrarLivroRequest req = CadastrarLivroRequest.newBuilder()
                            .setTitulo(titulos[i])
                            .setAutor("Robert C. Martin")
                            .setAno(2008 + i)
                            .setIsbn(isbns[i])
                            .build();

                    CadastrarLivroResponse res = blockingStub.cadastrarLivro(req);
                    System.out.println("Livro cadastrado ID: " + res.getId() + " - " + res.getMensagem());
                } catch (StatusRuntimeException e) {
                    System.err.println("Erro ao cadastrar: " + e.getStatus().getDescription());
                }
            }

            System.out.println("\n--- 2️⃣ TESTE: Tentar cadastrar ISBN Duplicado (Erro ALREADY_EXISTS) ---");
            try {
                CadastrarLivroRequest reqDuplicado = CadastrarLivroRequest.newBuilder()
                        .setTitulo("Clean Code Duplicado")
                        .setAutor("Outro Autor")
                        .setAno(2020)
                        .setIsbn("978-85-7522") // Mesmo ISBN do primeiro livro
                        .build();
                blockingStub.cadastrarLivro(reqDuplicado);
            } catch (StatusRuntimeException e) {
                System.out.println("Erro capturado com sucesso: " + e.getStatus().getCode() + " -> " + e.getStatus().getDescription());
            }

            System.out.println("\n--- 3️⃣ TESTE: Listar livros por autor cadastrado (Server Streaming) ---");
            try {
                ListarLivrosPorAutorRequest listReq = ListarLivrosPorAutorRequest.newBuilder()
                        .setAutor("Robert C. Martin")
                        .build();

                Iterator<Livro> livrosIt = blockingStub.listarLivrosPorAutor(listReq);
                while (livrosIt.hasNext()) {
                    Livro l = livrosIt.next();
                    System.out.println("Livro encontrado: [" + l.getIsbn() + "] " + l.getTitulo() + " (" + l.getAno() + ")");
                }
            } catch (StatusRuntimeException e) {
                System.err.println("Erro no streaming: " + e.getStatus().getDescription());
            }

            System.out.println("\n--- 4️⃣ TESTE: Listar livros de autor inexistente (Erro NOT_FOUND) ---");
            try {
                ListarLivrosPorAutorRequest listReqInexistente = ListarLivrosPorAutorRequest.newBuilder()
                        .setAutor("Autor Fantasma")
                        .build();

                Iterator<Livro> livrosIt = blockingStub.listarLivrosPorAutor(listReqInexistente);
                while (livrosIt.hasNext()) {
                    livrosIt.next();
                }
            } catch (StatusRuntimeException e) {
                System.out.println("Erro capturado com sucesso: " + e.getStatus().getCode() + " -> " + e.getStatus().getDescription());
            }

            System.out.println("\n--- 5️⃣ TESTE: Registrar 5 empréstimos (Client Streaming) ---");
            CountDownLatch finishLatch = new CountDownLatch(1);
            StreamObserver<EmprestimoResumo> responseObserver = new StreamObserver<EmprestimoResumo>() {
                @Override
                public void onNext(EmprestimoResumo resumo) {
                    System.out.println("Resumo recebido do Servidor:");
                    System.out.println("  Total de empréstimos: " + resumo.getTotalEmprestimos());
                    System.out.println("  Tempo de processamento: " + resumo.getTempoProcessamentoMs() + " ms");
                    System.out.println("  Mensagem: " + resumo.getMensagem());
                }

                @Override
                public void onError(Throwable t) {
                    System.err.println("Erro no client streaming: " + t.getMessage());
                    finishLatch.countDown();
                }

                @Override
                public void onCompleted() {
                    System.out.println("Client streaming concluído.");
                    finishLatch.countDown();
                }
            };

            StreamObserver<RegistrarEmprestimosRequest> requestObserver = asyncStub.registrarEmprestimos(responseObserver);
            for (int i = 1; i <= 5; i++) {
                requestObserver.onNext(RegistrarEmprestimosRequest.newBuilder()
                        .setUsuario("Aluno_" + i)
                        .setLivroId("livro-id-ficticio-" + i)
                        .build());
                Thread.sleep(200);
            }
            requestObserver.onCompleted();
            finishLatch.await(1, TimeUnit.SECONDS);

            System.out.println("\n--- 6️⃣ TESTE: Chat com bibliotecário (Bidirectional Streaming) ---");
            CountDownLatch chatLatch = new CountDownLatch(1);
            StreamObserver<ChatResponse> chatResponseObserver = new StreamObserver<ChatResponse>() {
                @Override
                public void onNext(ChatResponse value) {
                    System.out.println("[BIBLIOTECÁRIO VIRTUAL]: " + value.getRespostaBibliotecario() + " | Sugestão: " + value.getSugestaoLivro());
                }

                @Override
                public void onError(Throwable t) {
                    System.err.println("Erro no chat: " + t.getMessage());
                    chatLatch.countDown();
                }

                @Override
                public void onCompleted() {
                    System.out.println("Chat finalizado.");
                    chatLatch.countDown();
                }
            };

            StreamObserver<ChatRequest> chatRequestObserver = asyncStub.chatBibliotecario(chatResponseObserver);
            String[] mensagensChat = {
                "Olá, vocês têm livros de Java?",
                "Gostaria de algo sobre banco de dados.",
                "Obrigado pelas recomendações!"
            };

            for (String msg : mensagensChat) {
                System.out.println("[CLIENTE]: " + msg);
                chatRequestObserver.onNext(ChatRequest.newBuilder().setMensagem(msg).build());
                Thread.sleep(500);
            }
            chatRequestObserver.onCompleted();
            chatLatch.await(2, TimeUnit.SECONDS);

        } finally {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        }
    }
}