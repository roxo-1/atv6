package br.mackenzie.biblioteca.server;

import br.mackenzie.biblioteca.grpc.BibliotecaServiceGrpc;
import br.mackenzie.biblioteca.grpc.CadastrarLivroRequest;
import br.mackenzie.biblioteca.grpc.CadastrarLivroResponse;
import br.mackenzie.biblioteca.grpc.ChatRequest;
import br.mackenzie.biblioteca.grpc.ChatResponse;
import br.mackenzie.biblioteca.grpc.EmprestimoResumo;
import br.mackenzie.biblioteca.grpc.ListarLivrosPorAutorRequest;
import br.mackenzie.biblioteca.grpc.Livro;
import br.mackenzie.biblioteca.grpc.RegistrarEmprestimosRequest;
import br.mackenzie.biblioteca.model.Emprestimo;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class BibliotecaServiceImpl extends BibliotecaServiceGrpc.BibliotecaServiceImplBase {

    // Armazenamento em memória
    private final Map<String, Livro> acervoPorId = new ConcurrentHashMap<>();
    private final Map<String, String> isbnCadastrados = new ConcurrentHashMap<>();
    private final List<Emprestimo> emprestimos = new CopyOnWriteArrayList<>();

    // Palavra-chave -> {sugestão de livro, resposta do bibliotecário}
    private static final Map<String, String[]> SUGESTOES = new LinkedHashMap<>();

    static {
        SUGESTOES.put("java", new String[]{"Effective Java", "Temos excelentes títulos sobre desenvolvimento em Java!"});
        SUGESTOES.put("programação", new String[]{"Clean Code", "Boas práticas de programação são essenciais, veja esta sugestão."});
        SUGESTOES.put("programacao", new String[]{"Clean Code", "Boas práticas de programação são essenciais, veja esta sugestão."});
        SUGESTOES.put("banco", new String[]{"Sistemas de Bancos de Dados", "Aprenda modelagem relacional e NoSQL conosco."});
        SUGESTOES.put("dados", new String[]{"Sistemas de Bancos de Dados", "Aprenda modelagem relacional e NoSQL conosco."});
        SUGESTOES.put("arquitetura", new String[]{"Arquitetura Limpa", "Ótima escolha para quem quer estruturar bem um sistema."});
        SUGESTOES.put("distribuído", new String[]{"Sistemas Distribuídos: Princípios e Paradigmas", "Um clássico para entender comunicação entre serviços."});
        SUGESTOES.put("distribuido", new String[]{"Sistemas Distribuídos: Princípios e Paradigmas", "Um clássico para entender comunicação entre serviços."});
        SUGESTOES.put("grpc", new String[]{"gRPC: Up and Running", "Ideal para aprofundar em Protocol Buffers e tipos de RPC."});
        SUGESTOES.put("rede", new String[]{"Redes de Computadores", "Veja este título sobre protocolos e comunicação em rede."});
    }

    // ------------------------------------------------------------------
    // 1. Unary RPC
    // ------------------------------------------------------------------
    @Override
    public void cadastrarLivro(CadastrarLivroRequest request, StreamObserver<CadastrarLivroResponse> responseObserver) {
        System.out.println("[SERVIDOR] CadastrarLivro | titulo: " + request.getTitulo()
                + " | autor: " + request.getAutor()
                + " | ano: " + request.getAno()
                + " | isbn: " + request.getIsbn());

        // Validação dos campos
        if (request.getTitulo().isBlank() || request.getAutor().isBlank() || request.getIsbn().isBlank()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("Título, autor e ISBN são obrigatórios.")
                    .asRuntimeException());
            return;
        }
        if (request.getAno() <= 0) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("O ano deve ser um valor positivo.")
                    .asRuntimeException());
            return;
        }

        String isbn = request.getIsbn().trim();
        String id = UUID.randomUUID().toString();

        // putIfAbsent é atômico: evita dois cadastros simultâneos com o mesmo ISBN
        if (isbnCadastrados.putIfAbsent(isbn, id) != null) {
            responseObserver.onError(Status.ALREADY_EXISTS
                    .withDescription("Livro com o ISBN " + isbn + " já está cadastrado.")
                    .asRuntimeException());
            return;
        }

        Livro livro = Livro.newBuilder()
                .setId(id)
                .setTitulo(request.getTitulo().trim())
                .setAutor(request.getAutor().trim())
                .setAno(request.getAno())
                .setIsbn(isbn)
                .build();
        acervoPorId.put(id, livro);

        CadastrarLivroResponse response = CadastrarLivroResponse.newBuilder()
                .setId(id)
                .setMensagem("Livro cadastrado com sucesso!")
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    // ------------------------------------------------------------------
    // 2. Server Streaming RPC
    // ------------------------------------------------------------------
    @Override
    public void listarLivrosPorAutor(ListarLivrosPorAutorRequest request, StreamObserver<Livro> responseObserver) {
        System.out.println("[SERVIDOR] ListarLivrosPorAutor | autor: " + request.getAutor());

        if (request.getAutor().isBlank()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("O nome do autor é obrigatório.")
                    .asRuntimeException());
            return;
        }

        String autorBuscado = request.getAutor().trim();
        List<Livro> encontrados = new ArrayList<>();
        for (Livro livro : acervoPorId.values()) {
            if (livro.getAutor().equalsIgnoreCase(autorBuscado)) {
                encontrados.add(livro);
            }
        }

        if (encontrados.isEmpty()) {
            responseObserver.onError(Status.NOT_FOUND
                    .withDescription("Nenhum livro encontrado para o autor: " + autorBuscado)
                    .asRuntimeException());
            return;
        }

        encontrados.sort(Comparator.comparing(Livro::getTitulo));
        for (Livro livro : encontrados) {
            responseObserver.onNext(livro);
        }
        responseObserver.onCompleted();
    }

    // ------------------------------------------------------------------
    // 3. Client Streaming RPC
    // ------------------------------------------------------------------
    @Override
    public StreamObserver<RegistrarEmprestimosRequest> registrarEmprestimos(StreamObserver<EmprestimoResumo> responseObserver) {
        System.out.println("[SERVIDOR] RegistrarEmprestimos iniciado (Client Streaming)");
        final long inicio = System.currentTimeMillis();

        return new StreamObserver<RegistrarEmprestimosRequest>() {
            private int totalEmprestimos = 0;
            private boolean falhou = false;

            @Override
            public void onNext(RegistrarEmprestimosRequest request) {
                if (falhou) {
                    return; // a chamada já foi encerrada com erro
                }

                System.out.println("[SERVIDOR] Empréstimo recebido | usuario: " + request.getUsuario()
                        + " | livro_id: " + request.getLivroId());

                if (request.getUsuario().isBlank() || request.getLivroId().isBlank()) {
                    falhou = true;
                    responseObserver.onError(Status.INVALID_ARGUMENT
                            .withDescription("Usuário e livro_id são obrigatórios.")
                            .asRuntimeException());
                    return;
                }

                if (!acervoPorId.containsKey(request.getLivroId())) {
                    falhou = true;
                    responseObserver.onError(Status.NOT_FOUND
                            .withDescription("Livro não encontrado: " + request.getLivroId())
                            .asRuntimeException());
                    return;
                }

                emprestimos.add(new Emprestimo(request.getUsuario(), request.getLivroId(), Instant.now()));
                totalEmprestimos++;
            }

            @Override
            public void onError(Throwable t) {
                System.err.println("[SERVIDOR] Erro no client streaming: " + t.getMessage());
            }

            @Override
            public void onCompleted() {
                if (falhou) {
                    return; // a resposta de erro já foi enviada
                }

                long tempoTotal = System.currentTimeMillis() - inicio;
                EmprestimoResumo resumo = EmprestimoResumo.newBuilder()
                        .setTotalEmprestimos(totalEmprestimos)
                        .setTempoProcessamentoMs(tempoTotal)
                        .setMensagem("Todos os empréstimos foram processados com sucesso.")
                        .build();

                responseObserver.onNext(resumo);
                responseObserver.onCompleted();
                System.out.println("[SERVIDOR] Client streaming finalizado. Total: " + totalEmprestimos
                        + " (empréstimos acumulados no servidor: " + emprestimos.size() + ")");
            }
        };
    }

    // ------------------------------------------------------------------
    // 4. Bidirectional Streaming RPC
    // ------------------------------------------------------------------
    @Override
    public StreamObserver<ChatRequest> chatBibliotecario(StreamObserver<ChatResponse> responseObserver) {
        System.out.println("[SERVIDOR] ChatBibliotecario iniciado (Bidirectional Streaming)");

        return new StreamObserver<ChatRequest>() {
            @Override
            public void onNext(ChatRequest request) {
                System.out.println("[SERVIDOR] Chat - mensagem recebida: " + request.getMensagem());

                String texto = request.getMensagem().toLowerCase(Locale.ROOT);

                String sugestao = "Nenhum livro específico encontrado";
                String resposta = "Recebi sua mensagem. Pode me dizer um tema, como Java, banco de dados ou arquitetura?";

                for (Map.Entry<String, String[]> entrada : SUGESTOES.entrySet()) {
                    if (texto.contains(entrada.getKey())) {
                        sugestao = entrada.getValue()[0];
                        resposta = entrada.getValue()[1];
                        break;
                    }
                }

                ChatResponse chatResponse = ChatResponse.newBuilder()
                        .setSugestaoLivro(sugestao)
                        .setRespostaBibliotecario(resposta)
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
