package io.dogre.aerospike;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class Application {

    public static void main(String[] args) {
        ApplicationContext applicationContext = SpringApplication.run(Application.class, args);
        Server server = applicationContext.getBean(Server.class);
        server.start();
    }

    @Bean
    public ServiceHandler aerospkieMockServiceHandler(@Value("${aerospike.host}") String host,
            @Value("${aerospike.port}") int port, @Value("${aerospike.namespaces}") String namespaces) {
        return new ServiceHandlerImpl2(host + ":" + port, namespaces.split(","));
    }

    @Bean
    public Server aerospikeMockServer(@Value("${aerospike.port}") int port,
            @Value("${aerospike.io-threads}") int ioThreads, @Value("${aerospike.worker-threads}") int workerThread,
            ServiceHandler serviceHandler) {
        return new NettyServer(port, ioThreads, workerThread, serviceHandler);
    }

}
