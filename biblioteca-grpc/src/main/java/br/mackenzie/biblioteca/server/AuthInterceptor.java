package br.mackenzie.biblioteca.server;

import br.mackenzie.biblioteca.model.Constantes;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;

/**
 * Interceptor (bônus) de autenticação simples: exige o header
 * "authorization: Bearer &lt;token&gt;" em todas as chamadas.
 */
public class AuthInterceptor implements ServerInterceptor {

    private static final Metadata.Key<String> AUTH_KEY =
            Metadata.Key.of(Constantes.HEADER_AUTORIZACAO, Metadata.ASCII_STRING_MARSHALLER);

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call,
            Metadata headers,
            ServerCallHandler<ReqT, RespT> next) {

        String valor = headers.get(AUTH_KEY);

        if (valor == null || !valor.equals("Bearer " + Constantes.TOKEN)) {
            System.out.println("[AUTH] Chamada rejeitada (token ausente ou invalido): "
                    + call.getMethodDescriptor().getFullMethodName());
            call.close(Status.UNAUTHENTICATED.withDescription("Token ausente ou invalido."), new Metadata());
            return new ServerCall.Listener<ReqT>() {
            };
        }

        return next.startCall(call, headers);
    }
}
