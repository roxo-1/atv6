package br.mackenzie.biblioteca.server;

import io.grpc.ForwardingServerCall;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;

/**
 * Interceptor (bônus) que registra automaticamente todas as chamadas recebidas
 * e o status final de cada uma.
 */
public class LogInterceptor implements ServerInterceptor {

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call,
            Metadata headers,
            ServerCallHandler<ReqT, RespT> next) {

        final String metodo = call.getMethodDescriptor().getFullMethodName();
        System.out.println("[INTERCEPTOR] Chamada recebida: " + metodo);

        ServerCall<ReqT, RespT> chamadaComLog = new ForwardingServerCall.SimpleForwardingServerCall<ReqT, RespT>(call) {
            @Override
            public void close(Status status, Metadata trailers) {
                System.out.println("[INTERCEPTOR] Chamada finalizada: " + metodo + " | status: " + status.getCode());
                super.close(status, trailers);
            }
        };

        return next.startCall(chamadaComLog, headers);
    }
}
