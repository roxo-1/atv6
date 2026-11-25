package br.mackenzie.biblioteca.model;

import java.time.Instant;

/**
 * Empréstimo registrado em memória pelo servidor.
 */
public record Emprestimo(String usuario, String livroId, Instant registradoEm) {
}
