package com.asyncgate.signaling_server.config;

import com.asyncgate.signaling_server.infrastructure.client.MemberServiceClient;
import com.asyncgate.signaling_server.infrastructure.utility.WebClientUtil;
import com.asyncgate.signaling_server.security.filter.WebSocketHandshakeInterceptor;
import com.asyncgate.signaling_server.security.utility.JsonWebTokenUtil;
import com.asyncgate.signaling_server.signaling.KurentoManager;
import com.asyncgate.signaling_server.support.handler.KurentoHandler;
import org.kurento.client.KurentoClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;
import org.springframework.web.socket.server.standard.ServletServerContainerFactoryBean;

@Configuration
@EnableWebSocket
public class KurentoConfig implements WebSocketConfigurer {

    @Value("${kms.url}")
    private String kmsUrl;

    private final WebSocketHandshakeInterceptor webSocketHandshakeInterceptor;
    private final JsonWebTokenUtil jsonWebTokenUtil;  // 추가

    private final WebClientUtil webClientUtil;

    public KurentoConfig(WebSocketHandshakeInterceptor webSocketHandshakeInterceptor, JsonWebTokenUtil jsonWebTokenUtil, WebClientUtil webClientUtil) {
        this.webSocketHandshakeInterceptor = webSocketHandshakeInterceptor;
        this.jsonWebTokenUtil = jsonWebTokenUtil;  // 추가
        this.webClientUtil = webClientUtil;
    }

    @Bean
    public KurentoClient kurentoClient() {
        return KurentoClient.create(kmsUrl);
    }

    @Bean
    public MemberServiceClient memberServiceClient() {
        return new MemberServiceClient(webClientUtil);
    }

    @Bean
    public KurentoManager kurentoManager(KurentoClient kurentoClient, MemberServiceClient memberServiceClient) {
        return new KurentoManager(kurentoClient, memberServiceClient);
    }

    @Bean
    public KurentoHandler kurentoHandler(KurentoManager kurentoManager) {
        return new KurentoHandler(kurentoManager, jsonWebTokenUtil); // JsonWebTokenUtil 주입
    }

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        System.out.println("🚀 WebSocketHandlerRegistry 등록");
        registry.addHandler(kurentoHandler(kurentoManager(kurentoClient(), memberServiceClient())), "/signal")
                .setAllowedOriginPatterns("*")
                .addInterceptors(webSocketHandshakeInterceptor);
    }

    @Bean
    public ServletServerContainerFactoryBean createWebSocketContainer() {
        ServletServerContainerFactoryBean container = new ServletServerContainerFactoryBean();
        container.setMaxTextMessageBufferSize(32768);
        container.setMaxBinaryMessageBufferSize(32768);
        return container;
    }
}