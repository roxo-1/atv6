package br.mackenzie.biblioteca.client;

import br.mackenzie.biblioteca.grpc.BibliotecaServiceGrpc;
import br.mackenzie.biblioteca.grpc.CadastrarLivroRequest;
import br.mackenzie.biblioteca.grpc.CadastrarLivroResponse;
import br.mackenzie.biblioteca.grpc.ChatRequest;
import br.mackenzie.biblioteca.grpc.ChatResponse;
import br.mackenzie.biblioteca.grpc.EmprestimoResumo;
import br.mackenzie.biblioteca.grpc.ListarLivrosPorAutorRequest;
import br.mackenzie.biblioteca.grpc.Livro;
import br.mackenzie.biblioteca.grpc.RegistrarEmprestimosRequest;
import br.mackenzie.biblioteca.model.Constantes;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ClienteBiblioteca {

    /** Deadline (bônus) aplicado a todas as chamadas do cliente. */
    private static final int DEADLINE_SEGUNDOS = 10;

    private static ManagedChannel channel;

    private static BibliotecaServiceGrpc.BibliotecaServiceBlockingStub blocking() {
        return BibliotecaServiceGrpc.newBlockingStub(channel)
                .withDeadlineAfter(DEADLINE_SEGUNDOS, TimeUnit.SECONDS);
    }

    private static BibliotecaServiceGrpc.BibliotecaServiceStub async() {
        return BibliotecaServiceGrpc.newStub(channel)
                .withDeadlineAfter(DEADLINE_SEGUNDOS, TimeUnit.SECONDS);
    }

    private static String descreverErro(StatusRuntimeException e) {
        return e.getStatus().getCode() + " -> " + e.getStatus().getDescription();
    }

    public static void main(String[] args) throws InterruptedException {
        String target = args.length > 0 ? args[0] : "localhost:" + Constantes.PORTA_PADRAO;

        // Autenticação simples (bônus): token enviado no header "authorization"
        Metadata headers = new Metadata();
        headers.put(Metadata.Key.of(Constantes.HEADER_AUTORIZACAO, Metadata.ASCII_STRING_MARSHALLER),
                "Bearer " + Constantes.TOKEN);

        channel = ManagedChannelBuilder.forTarget(target)
                .usePlaintext()
                .intercept(MetadataUtils.newAttachHeadersInterceptor(headers))
                .build();

        List<String> idsCadastrados = new ArrayList<>();

        try {
            // ----------------------------------------------------------------
            System.out.println("--- TESTE 1: Cadastrar 3 livros (Unary) ---");
            String[] isbns = {"978-85-7522", "978-01-3468", "978-85-5080"};
            String[] titulos = {"Clean Code", "Effective Java", "Arquitetura Limpa"};
            String[] autores = {"Robert C. Martin", "Joshua Bloch", "Robert C. Martin"};
            int[] anos = {2008, 2018, 2019};

            for (int i = 0; i < 3; i++) {
                try {
                    CadastrarLivroRequest req = CadastrarLivroRequest.newBuilder()
                            .setTitulo(titulos[i])
                            .setAutor(autores[i])
                            .setAno(anos[i])
                            .setIsbn(isbns[i])
                            .build();

                    CadastrarLivroResponse res = blocking().cadastrarLivro(req);
                    idsCadastrados.add(res.getId());
                    System.out.println("Livro cadastrado | ID: " + res.getId() + " | " + res.getMensagem());
                } catch (StatusRuntimeException e) {
                    System.err.println("Erro ao cadastrar: " + descreverErro(e));
                }
            }

            // ----------------------------------------------------------------
            System.out.println("\n--- TESTE 2: Listar livros de autor cadastrado (Server Streaming) ---");
            try {
                ListarLivrosPorAutorRequest listReq = ListarLivrosPorAutorRequest.newBuilder()
                        .setAutor("Robert C. Martin")
                        .build();

                Iterator<Livro> livrosIt = blocking().listarLivrosPorAutor(listReq);
                while (livrosIt.hasNext()) {
                    Livro l = livrosIt.next();
                    System.out.println("Livro encontrado: [" + l.getIsbn() + "] " + l.getTitulo()
                            + " (" + l.getAno() + ") - " + l.getAutor());
                }
            } catch (StatusRuntimeException e) {
                System.err.println("Erro no streaming: " + descreverErro(e));
            }

            // ----------------------------------------------------------------
            System.out.println("\n--- TESTE 3: Listar livros de autor inexistente (esperado: NOT_FOUND) ---");
            try {
                ListarLivrosPorAutorRequest listReqInexistente = ListarLivrosPorAutorRequest.newBuilder()
                        .setAutor("Autor Fantasma")
                        .build();

                Iterator<Livro> livrosIt = blocking().listarLivrosPorAutor(listReqInexistente);
                while (livrosIt.hasNext()) {
                    livrosIt.next();
                }
                System.out.println("Stream vazia (nenhum livro retornado).");
            } catch (StatusRuntimeException e) {
                System.out.println("Erro capturado com sucesso: " + descreverErro(e));
            }

            // ----------------------------------------------------------------
            System.out.println("\n--- TESTE 4: Registrar 5 empréstimos (Client Streaming) ---");
            if (idsCadastrados.isEmpty()) {
                System.err.println("Nenhum livro foi cadastrado; não é possível registrar empréstimos.");
            } else {
                CountDownLatch emprestimosLatch = new CountDownLatch(1);
                StreamObserver<EmprestimoResumo> resumoObserver = new StreamObserver<EmprestimoResumo>() {
                    @Override
                    public void onNext(EmprestimoResumo resumo) {
                        System.out.println("Resumo recebido do servidor:");
                        System.out.println("  Total de empréstimos: " + resumo.getTotalEmprestimos());
                        System.out.println("  Tempo de processamento: " + resumo.getTempoProcessamentoMs() + " ms");
                        System.out.println("  Mensagem: " + resumo.getMensagem());
                    }

                    @Override
                    public void onError(Throwable t) {
                        System.err.println("Erro no client streaming: " + Status.fromThrowable(t));
                        emprestimosLatch.countDown();
                    }

                    @Override
                    public void onCompleted() {
                        System.out.println("Client streaming concluído.");
                        emprestimosLatch.countDown();
                    }
                };

                StreamObserver<RegistrarEmprestimosRequest> emprestimosRequest = async().registrarEmprestimos(resumoObserver);
                for (int i = 1; i <= 5; i++) {
                    String livroId = idsCadastrados.get((i - 1) % idsCadastrados.size());
                    System.out.println("Enviando empréstimo " + i + " | usuario: Aluno_" + i + " | livro_id: " + livroId);
                    emprestimosRequest.onNext(RegistrarEmprestimosRequest.newBuilder()
                            .setUsuario("Aluno_" + i)
                            .setLivroId(livroId)
                            .build());
                    Thread.sleep(200);
                }
                emprestimosRequest.onCompleted();
                emprestimosLatch.await(15, TimeUnit.SECONDS);
            }

            // ----------------------------------------------------------------
            System.out.println("\n--- TESTE 5: Chat com o bibliotecário (Bidirectional Streaming) ---");
            CountDownLatch chatLatch = new CountDownLatch(1);
            StreamObserver<ChatResponse> chatResponseObserver = new StreamObserver<ChatResponse>() {
                @Override
                public void onNext(ChatResponse value) {
                    System.out.println("[BIBLIOTECÁRIO VIRTUAL]: " + value.getRespostaBibliotecario()
                            + " | Sugestão: " + value.getSugestaoLivro());
                }

                @Override
                public void onError(Throwable t) {
                    System.err.println("Erro no chat: " + Status.fromThrowable(t));
                    chatLatch.countDown();
                }

                @Override
                public void onCompleted() {
                    System.out.println("Chat finalizado.");
                    chatLatch.countDown();
                }
            };

            StreamObserver<ChatRequest> chatRequest = async().chatBibliotecario(chatResponseObserver);
            String[] mensagensChat = {
                    "Olá, vocês têm livros de Java?",
                    "Gostaria de algo sobre banco de dados.",
                    "Pode sugerir algo sobre arquitetura de software?"
            };

            for (String msg : mensagensChat) {
                System.out.println("[CLIENTE]: " + msg);
                chatRequest.onNext(ChatRequest.newBuilder().setMensagem(msg).build());
                Thread.sleep(500);
            }
            chatRequest.onCompleted();
            chatLatch.await(15, TimeUnit.SECONDS);

            // ----------------------------------------------------------------
            System.out.println("\n--- TESTE 6: Cadastrar livro com ISBN duplicado (esperado: ALREADY_EXISTS) ---");
            try {
                CadastrarLivroRequest reqDuplicado = CadastrarLivroRequest.newBuilder()
                        .setTitulo("Clean Code Duplicado")
                        .setAutor("Outro Autor")
                        .setAno(2020)
                        .setIsbn("978-85-7522") // mesmo ISBN do primeiro livro
                        .build();
                blocking().cadastrarLivro(reqDuplicado);
                System.err.println("ERRO: o servidor deveria ter rejeitado o ISBN duplicado.");
            } catch (StatusRuntimeException e) {
                System.out.println("Erro capturado com sucesso: " + descreverErro(e));
            }

            // ----------------------------------------------------------------
            System.out.println("\n--- TESTE EXTRA 7: Cadastrar livro sem título/ISBN (esperado: INVALID_ARGUMENT) ---");
            try {
                blocking().cadastrarLivro(CadastrarLivroRequest.newBuilder()
                        .setTitulo("")
                        .setAutor("Alguém")
                        .setAno(2021)
                        .setIsbn("")
                        .build());
                System.err.println("ERRO: o servidor deveria ter rejeitado a requisição inválida.");
            } catch (StatusRuntimeException e) {
                System.out.println("Erro capturado com sucesso: " + descreverErro(e));
            }

            // ----------------------------------------------------------------
            System.out.println("\n--- TESTE EXTRA 8: Empréstimo de livro inexistente (esperado: NOT_FOUND) ---");
            CountDownLatch erroLatch = new CountDownLatch(1);
            StreamObserver<EmprestimoResumo> erroObserver = new StreamObserver<EmprestimoResumo>() {
                @Override
                public void onNext(EmprestimoResumo resumo) {
                    System.err.println("ERRO: o servidor deveria ter rejeitado o livro inexistente.");
                }

                @Override
                public void onError(Throwable t) {
                    Status status = Status.fromThrowable(t);
                    System.out.println("Erro capturado com sucesso: " + status.getCode() + " -> " + status.getDescription());
                    erroLatch.countDown();
                }

                @Override
                public void onCompleted() {
                    erroLatch.countDown();
                }
            };
            StreamObserver<RegistrarEmprestimosRequest> erroRequest = async().registrarEmprestimos(erroObserver);
            try {
                erroRequest.onNext(RegistrarEmprestimosRequest.newBuilder()
                        .setUsuario("Aluno_X")
                        .setLivroId("id-que-nao-existe")
                        .build());
                erroRequest.onCompleted();
            } catch (RuntimeException e) {
                // a chamada pode já ter sido encerrada pelo servidor; o erro chega no onError acima
            }
            erroLatch.await(10, TimeUnit.SECONDS);

            // ----------------------------------------------------------------
            System.out.println("\n--- TESTE EXTRA 9: Chamada sem token (esperado: UNAUTHENTICATED) ---");
            ManagedChannel canalSemToken = ManagedChannelBuilder.forTarget(target).usePlaintext().build();
            try {
                BibliotecaServiceGrpc.newBlockingStub(canalSemToken)
                        .withDeadlineAfter(DEADLINE_SEGUNDOS, TimeUnit.SECONDS)
                        .cadastrarLivro(CadastrarLivroRequest.newBuilder()
                                .setTitulo("Livro Sem Token")
                                .setAutor("Anônimo")
                                .setAno(2022)
                                .setIsbn("000-00-0000")
                                .build());
                System.err.println("ERRO: o servidor deveria ter rejeitado a chamada sem token.");
            } catch (StatusRuntimeException e) {
                System.out.println("Erro capturado com sucesso: " + descreverErro(e));
            } finally {
                canalSemToken.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            }

        } finally {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        }
    }
}
