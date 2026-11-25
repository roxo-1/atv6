package br.mackenzie.biblioteca.model;

/**
 * Valores compartilhados entre servidor e cliente.
 */
public final class Constantes {

    private Constantes() {
    }

    public static final int PORTA_PADRAO = 50051;

    /** Nome do header (metadata) usado na autenticação simples. */
    public static final String HEADER_AUTORIZACAO = "authorization";

    /** Token aceito pelo servidor (autenticação simples, apenas para fins didáticos). */
    public static final String TOKEN = "biblioteca-token-123";
}
