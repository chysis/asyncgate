package com.asyncgate.signaling_server.security.constant;

import java.util.List;

public class Constants {

    // JWT
    public static String MEMBER_ID_ATTRIBUTE_NAME = "MEMBER_ID";
    public static String MEMBER_ID_CLAIM_NAME = "mid";

    // HEADER
    public static String BEARER_PREFIX = "Bearer ";
    public static String AUTHORIZATION_HEADER = "Authorization";


    /**
     * 인증이 필요 없는 URL
     */
    public static List<String> NO_NEED_AUTH_URLS = List.of(
            // Authentication/Authorization
            "/", // root
            "/actuator/info",
            "/health",
            "/index.html",
            "/signal",
            "/ice",

            // Swagger
            "/api-docs.html",
            "/api-docs/**",
            "/swagger-ui/**",
            "/v3/**"
    );

    /**
     * Swagger 에서 사용하는 URL
     */
    public static List<String> SWAGGER_URLS = List.of(
            "/api-docs.html",
            "/api-docs",
            "/swagger-ui",
            "/v3"
    );
}