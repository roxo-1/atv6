package br.mackenzie.biblioteca.server;

import br.mackenzie.biblioteca.grpc.*;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class BibliotecaServiceImpl extends BibliotecaServiceGrpc.BibliotecaServiceImplBase {

    // Armazenamento em memória
    private final Map<String, Livro> acervoPorId = new ConcurrentHashMap<>();
    private final Map<String, String> isbnCadastrados = new ConcurrentHashMap<>();

    @Override
    public void cadastrarLivro(CadastrarLivroRequest request, StreamObserver<CadastrarLivroResponse> responseObserver) {
        System.out.println("[SERVIDOR] Chamada recebida: CadastrarLivro | ISBN: " + request.getIsbn());

        // Validação de ISBN duplicado
        if (isbnCadastrados.containsKey(request.getIsbn())) {
            responseObserver.onError(Status.ALREADY_EXISTS
                    .withDescription("Livro com o ISBN " + request.getIsbn() + " já está cadastrado.")
                    .asRuntimeException());
            return;
        }

        String id = UUID.randomUUID().toString();
        Livro livro = Livro.newBuilder()
                .setId(id)
                .setTitulo(request.getTitulo())
                .setAutor(request.getAutor())
                .setAno(request.getAno())
                .setIsbn(request.getIsbn())
                .build();

        acervoPorId.put(id, livro);
        isbnCadastrados.put(request.getIsbn(), id);

        CadastrarLivroResponse response = CadastrarLivroResponse.newBuilder()
                .setId(id)
                .setMensagem("Livro cadastrado com sucesso!")
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void listarLivrosPorAutor(ListarLivrosPorAutorRequest request, StreamObserver<Livro> responseObserver) {
        System.out.println("[SERVIDOR] Chamada recebida: ListarLivrosPorAutor | Autor: " + request.getAutor());

        List<Livro> encontrados = new ArrayList<>();
        for (Livro livro : acervoPorId.values()) {
            if (livro.getAutor().equalsIgnoreCase(request.getAutor())) {
                encontrados.add(livro);
            }
        }

        if (encontrados.isEmpty()) {
            responseObserver.onError(Status.NOT_FOUND
                    .withDescription("Nenhum livro encontrado para o autor: " + request.getAutor())
                    .asRuntimeException());
            return;
        }

        for (Livro livro : encontrados) {
            responseObserver.onNext(livro);
        }
        responseObserver.onCompleted();
    }

    @Override
    public StreamObserver<RegistrarEmprestimosRequest> registrarEmprestimos(StreamObserver<EmprestimoResumo> responseObserver) {
        System.out.println("[SERVIDOR] Chamada iniciada: RegistrarEmprestimos (Client Streaming)");
        long startTime = System.currentTimeMillis();

        return new StreamObserver<RegistrarEmprestimosRequest>() {
            int totalEmprestimos = 0;

            @Override
            public void onNext(RegistrarEmprestimosRequest request) {
                System.out.println("[SERVIDOR] Empréstimo recebido -> Usuário: " + request.getUsuario() + ", Livro ID: " + request.getLivroId());
                totalEmprestimos++;
            }

            @Override
            public void onError(Throwable t) {
                System.err.println("[SERVIDOR] Erro no client streaming: " + t.getMessage());
            }

            @Override
            public void onCompleted() {
                long tempoTotal = System.currentTimeMillis() - startTime;
                EmprestimoResumo resumo = EmprestimoResumo.newBuilder()
                        .setTotalEmprestimos(totalEmprestimos)
                        .setTempoProcessamentoMs(tempoTotal)
                        .setMensagem("Todos os empréstimos foram processados com sucesso.")
                        .build();

                responseObserver.onNext(resumo);
                responseObserver.onCompleted();
                System.out.println("[SERVIDOR] Client streaming finalizado. Total: " + totalEmprestimos);
            }
        };
    }

    @Override
    public StreamObserver<ChatRequest> chatBibliotecario(StreamObserver<ChatResponse> responseObserver) {
        System.out.println("[SERVIDOR] Chamada iniciada: ChatBibliotecario (Bidirectional Streaming)");

        return new StreamObserver<ChatRequest>() {
            @Override
            public void onNext(ChatRequest request) {
                String mensagemUsuario = request.getMensagem().toLowerCase();
                System.out.println("[SERVIDOR] Chat - Mensagem recebida: " + request.getMensagem());

                String sugestao = "Nenhum livro específico encontrado";
                String resposta = "Recebi sua mensagem sobre '" + request.getMensagem() + "'.";

                if (mensagemUsuario.contains("java") || mensagemUsuario.contains("programação")) {
                    sugestao = "Java: Guia do Programador";
                    resposta = "Temos excelentes títulos sobre desenvolvimento em Java!";
                } else if (mensagemUsuario.contains("banco") || mensagemUsuario.contains("dados")) {
                    sugestao = "Sistemas de Bancos de Dados";
                    resposta = "Aprenda modelagem relacional e NoSQL conosco.";
                }

                ChatResponse chatResponse = ChatResponse.newBuilder()
                        .setSugestaoLivro(sugestao)
                        .respostaBibliotecario(resposta) // Ajustar conforme gerado pelo Protobuf (ex: setRespostaBibliotecario)
                        .build();

                responseObserver.onNext(chatResponse);
            }

            @Override
            public void onError(Throwable t) {
                System.err.println("[SERVIDOR] Erro no chat: " + t.getMessage());
            }

            @Override
            public void onCompleted() {
                System.out.println("[SERVIDOR] Chat encerrado pelo cliente.");
                responseObserver.onCompleted();
            }
        };
    }
}