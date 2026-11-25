# Biblioteca Digital com gRPC (Java 17 + Maven)

**Disciplina:** Computação Distribuída — Atividade Avaliativa 6
**Tema:** Comunicação entre serviços com gRPC e Protocol Buffers

## Alunos

| Nome completo | RA |
|---|---|
| **Ana Julia Yaguti Matilha** | **10436655** |
| **Carolina Lee** | **10440304** |
| **Pedro Casas Pequeno Junior** | **10437031** |


## Descrição do projeto

Backend de um sistema de gerenciamento de biblioteca digital. Um servidor gRPC central (`BibliotecaService`) expõe quatro operações, cada uma usando um tipo diferente de comunicação gRPC:

| RPC | Tipo | O que faz |
|---|---|---|
| `CadastrarLivro` | Unary | Cadastra um livro e devolve o ID gerado |
| `ListarLivrosPorAutor` | Server Streaming | Devolve, em stream, os livros de um autor |
| `RegistrarEmprestimos` | Client Streaming | Recebe vários empréstimos e devolve um resumo (total e tempo de processamento) |
| `ChatBibliotecario` | Bidirectional Streaming | Para cada mensagem do usuário, sugere um livro com base em palavras-chave |

A persistência é em memória (`Map` / `List`), sem banco de dados.

### Tratamento de erros (`Status` do gRPC)

| Situação | Status |
|---|---|
| ISBN já cadastrado | `ALREADY_EXISTS` |
| Autor sem livros cadastrados | `NOT_FOUND` |
| Empréstimo de livro que não existe | `NOT_FOUND` |
| Campos obrigatórios vazios / ano inválido | `INVALID_ARGUMENT` |
| Chamada sem token válido | `UNAUTHENTICATED` |
| Prazo da chamada estourado | `DEADLINE_EXCEEDED` |

### Bônus implementados

- **Interceptor de log** (`LogInterceptor`): registra automaticamente todas as chamadas e o status final.
- **Deadline** no cliente: todas as chamadas têm prazo de 10 segundos.
- **Autenticação simples via metadata** (`AuthInterceptor`): o cliente envia `authorization: Bearer <token>`.

## Estrutura do projeto

```text
biblioteca-grpc/
├── pom.xml
├── docs/ENUNCIADO.md
└── src/main/
    ├── proto/biblioteca.proto
    └── java/br/mackenzie/biblioteca/
        ├── server/
        │   ├── ServidorBiblioteca.java
        │   ├── BibliotecaServiceImpl.java
        │   ├── LogInterceptor.java
        │   └── AuthInterceptor.java
        ├── client/
        │   └── ClienteBiblioteca.java
        └── model/
            ├── Constantes.java
            └── Emprestimo.java
```

## Pré-requisitos

- Java 17 ou superior (`java -version`)
- Maven 3.8+ (`mvn -version`)
- Acesso à internet na primeira compilação (o Maven baixa as dependências e o `protoc`)

## Como compilar

```bash
mvn clean compile      # gera as classes do .proto e compila
mvn clean package      # compila e empacota (usado na avaliação)
```

## Como executar

Use **dois terminais**, ambos na pasta biblioteca-grpc/ do projeto.

**Terminal 1 — servidor (suba primeiro):**

```bash
mvn exec:java -Dexec.mainClass=br.mackenzie.biblioteca.server.ServidorBiblioteca
```

**Terminal 2 — cliente (executa todos os testes do roteiro):**

```bash
mvn exec:java -Dexec.mainClass=br.mackenzie.biblioteca.client.ClienteBiblioteca
```

Para reiniciar os testes do zero, pare o servidor (`Ctrl+C`) e suba-o novamente, pois o acervo fica só em memória (rodar o cliente duas vezes na mesma sessão do servidor faz o cadastro retornar `ALREADY_EXISTS`).

> Se os acentos aparecerem trocados no Windows, rode `chcp 65001` no terminal antes de executar.

## Saída esperada

> Os IDs são UUIDs aleatórios e o tempo de processamento varia. **Substitua pelos prints reais da sua execução.**

**Cliente:**

```text
--- TESTE 1: Cadastrar 3 livros (Unary) ---
Livro cadastrado | ID: <uuid> | Livro cadastrado com sucesso!
Livro cadastrado | ID: <uuid> | Livro cadastrado com sucesso!
Livro cadastrado | ID: <uuid> | Livro cadastrado com sucesso!

--- TESTE 2: Listar livros de autor cadastrado (Server Streaming) ---
Livro encontrado: [978-85-5080] Arquitetura Limpa (2019) - Robert C. Martin
Livro encontrado: [978-85-7522] Clean Code (2008) - Robert C. Martin

--- TESTE 3: Listar livros de autor inexistente (esperado: NOT_FOUND) ---
Erro capturado com sucesso: NOT_FOUND -> Nenhum livro encontrado para o autor: Autor Fantasma

--- TESTE 4: Registrar 5 empréstimos (Client Streaming) ---
Enviando empréstimo 1 | usuario: Aluno_1 | livro_id: <uuid>
... (5 envios)
Resumo recebido do servidor:
  Total de empréstimos: 5
  Tempo de processamento: ~1000 ms
  Mensagem: Todos os empréstimos foram processados com sucesso.
Client streaming concluído.

--- TESTE 5: Chat com o bibliotecário (Bidirectional Streaming) ---
[CLIENTE]: Olá, vocês têm livros de Java?
[BIBLIOTECÁRIO VIRTUAL]: Temos excelentes títulos sobre desenvolvimento em Java! | Sugestão: Effective Java
[CLIENTE]: Gostaria de algo sobre banco de dados.
[BIBLIOTECÁRIO VIRTUAL]: Aprenda modelagem relacional e NoSQL conosco. | Sugestão: Sistemas de Bancos de Dados
[CLIENTE]: Pode sugerir algo sobre arquitetura de software?
[BIBLIOTECÁRIO VIRTUAL]: Ótima escolha para quem quer estruturar bem um sistema. | Sugestão: Arquitetura Limpa
Chat finalizado.

--- TESTE 6: Cadastrar livro com ISBN duplicado (esperado: ALREADY_EXISTS) ---
Erro capturado com sucesso: ALREADY_EXISTS -> Livro com o ISBN 978-85-7522 já está cadastrado.

--- TESTE EXTRA 7: ... (esperado: INVALID_ARGUMENT) ---
Erro capturado com sucesso: INVALID_ARGUMENT -> Título, autor e ISBN são obrigatórios.

--- TESTE EXTRA 8: ... (esperado: NOT_FOUND) ---
Erro capturado com sucesso: NOT_FOUND -> Livro não encontrado: id-que-nao-existe

--- TESTE EXTRA 9: ... (esperado: UNAUTHENTICATED) ---
Erro capturado com sucesso: UNAUTHENTICATED -> Token ausente ou invalido.
```

**Servidor (trecho):**

```text
Servidor Biblioteca gRPC iniciado na porta 50051
[INTERCEPTOR] Chamada recebida: biblioteca.BibliotecaService/CadastrarLivro
[SERVIDOR] CadastrarLivro | titulo: Clean Code | autor: Robert C. Martin | ano: 2008 | isbn: 978-85-7522
[INTERCEPTOR] Chamada finalizada: biblioteca.BibliotecaService/CadastrarLivro | status: OK
...
[AUTH] Chamada rejeitada (token ausente ou invalido): biblioteca.BibliotecaService/CadastrarLivro
```
