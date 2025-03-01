package com.asyncgate.signaling_server.security.filter;

import com.asyncgate.signaling_server.security.constant.Constants;
import com.asyncgate.signaling_server.security.utility.JsonWebTokenUtil;
import io.jsonwebtoken.Claims;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.server.ServerHttpRequest;
import org.springframework.http.server.ServerHttpResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.WebSocketHandler;
import org.springframework.web.socket.server.HandshakeInterceptor;

import java.util.List;
import java.util.Map;

@Component
@RequiredArgsConstructor
public class WebSocketHandshakeInterceptor implements HandshakeInterceptor {

    private static final Logger log = LoggerFactory.getLogger(WebSocketHandshakeInterceptor.class);

    private final JsonWebTokenUtil jsonWebTokenUtil;

    /**
     * Sec-WebSocket-Protocol 헤더 값은 "v10.stomp, <JWT 토큰>" 형태로 요청 옴.
     * 첫 번째 값은 "v10.stomp", 두 번째 값은 JWT 토큰을 반환한다.
     */
    private String[] splitProtocolHeader(String headerValue) {
        if (headerValue == null || headerValue.isBlank()) {
            return null;
        }
        String[] parts = headerValue.split(",");
        if (parts.length < 2) {
            return null;
        }
        if (!"v10.stomp".equals(parts[0].trim())) {
            return null;
        }
        return new String[]{parts[0].trim(), parts[1].trim()};
    }

    @Override
    public boolean beforeHandshake(ServerHttpRequest request, ServerHttpResponse response,
                                   WebSocketHandler wsHandler, Map<String, Object> attributes) {
        log.info("✅ WebSocket Handshake - JWT 검증 시작");
        HttpHeaders headers = request.getHeaders();
        log.info("headers.size = {}", headers.size());

        for (Map.Entry<String, List<String>> entry : headers.entrySet()) {
            log.info("header = {} : {}", entry.getKey(), entry.getValue());
        }

        List<String> protocols = headers.get("Sec-WebSocket-Protocol");
        if (protocols == null || protocols.isEmpty()) {
            log.info("❌ STOMP 프로토콜 없음: WebSocket 연결 거부");
            response.setStatusCode(HttpStatus.BAD_REQUEST);
            return false;
        }

        // 클라이언트가 보낸 헤더의 첫 번째 값 예: "v10.stomp, <JWT 토큰>"
        String rawProtocol = protocols.get(0);
        String[] pair = splitProtocolHeader(rawProtocol);
        if (pair == null) {
            log.info("❌ 형식 오류: 헤더가 'v10.stomp, <JWT>' 형태가 아님");
            response.setStatusCode(HttpStatus.BAD_REQUEST);
            return false;
        }
        String jwtToken = pair[1];

        Claims claims = jsonWebTokenUtil.validate(jwtToken);

        String memberId = claims.get(Constants.MEMBER_ID_CLAIM_NAME, String.class);
        attributes.put("userId", memberId);  // 사용자 정보 저장

        log.info("✅ WebSocket Handshake 성공 - userId: {}", memberId);

        return true;
    }

    @Override
    public void afterHandshake(ServerHttpRequest request, ServerHttpResponse response,
                               WebSocketHandler wsHandler, Exception exception) {
        // After handshake logic (if needed)
    }
}