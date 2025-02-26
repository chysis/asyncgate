package com.asyncgate.signaling_server.support.handler;

import com.asyncgate.signaling_server.dto.response.GetUsersInChannelResponse;
import com.asyncgate.signaling_server.signaling.KurentoManager;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import lombok.extern.slf4j.Slf4j;
import org.kurento.client.IceCandidate;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.*;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

@Slf4j
@Component
public class KurentoHandler extends TextWebSocketHandler {

    private final KurentoManager kurentoManager;
    private final Map<String, WebSocketSession> sessions = new ConcurrentHashMap<>();

    public KurentoHandler(KurentoManager kurentoManager) {
        this.kurentoManager = kurentoManager;
    }

    /**
     * 클라이언트가 WebSocket에 연결되었을 때 실행됨
     */
    @Override
    public void afterConnectionEstablished(WebSocketSession session) {
        sessions.put(session.getId(), session);
        log.info("✅ WebSocket 연결됨: {}", session.getId());
    }

    /**
     * WebSocket 메시지를 처리하는 핵심 메서드
     */
    @Override
    public void handleTextMessage(WebSocketSession session, TextMessage message) throws IOException {
        JsonObject jsonMessage = new Gson().fromJson(message.getPayload(), JsonObject.class);
        log.info("📩 WebSocket 메시지 수신: {}", jsonMessage);

        if (!jsonMessage.has("id")) {
            log.error("❌ WebSocket 메시지 오류: id 필드 없음");
            return;
        }

        String messageType = jsonMessage.get("id").getAsString();
        String roomId = jsonMessage.has("roomId") ? jsonMessage.get("roomId").getAsString() : null;
        String userId = jsonMessage.has("userId") ? jsonMessage.get("userId").getAsString() : null;

        if (roomId == null || userId == null) {
            log.error("❌ WebSocket 메시지 오류: roomId 또는 userId가 null");
            return;
        }

        switch (messageType) {
            case "getUsers":
                sendUsersInChannel(session, roomId);
                break;
            case "start":
                handleStart(session, roomId, userId, jsonMessage);
                break;
            case "candidate":
                handleIceCandidate(roomId, userId, jsonMessage);
                break;
            case "toggleAudio":
                toggleMediaState(roomId, userId, "audio", jsonMessage.get("enabled").getAsBoolean());
                break;
            case "toggleVideo":
                toggleMediaState(roomId, userId, "video", jsonMessage.get("enabled").getAsBoolean());
                break;
            case "toggleScreen":
                toggleMediaState(roomId, userId, "screen", jsonMessage.get("enabled").getAsBoolean());
                break;
            default:
                log.warn("⚠️ 알 수 없는 WebSocket 메시지 유형: {}", messageType);
        }
    }

    /**
     * 특정 채널의 모든 유저 정보를 클라이언트에게 반환
     */
    private void sendUsersInChannel(WebSocketSession session, String roomId) {
        List<GetUsersInChannelResponse.UserInRoom> users = kurentoManager.getUsersInChannel(roomId);
        GetUsersInChannelResponse response = GetUsersInChannelResponse.builder()
                .channelId(roomId)
                .users(users)
                .build();

        try {
            session.sendMessage(new TextMessage(new Gson().toJson(response)));
            log.info("📡 [Kurento] 채널 유저 정보 전송: {}", roomId);
        } catch (IOException e) {
            log.error("❌ WebSocket 메시지 전송 실패: {}", e.getMessage());
        }
    }

    /**
     * 사용자가 WebRTC 연결을 시작할 때 처리 (SDP Offer → SDP Answer 반환)
     */
    private void handleStart(WebSocketSession session, String roomId, String userId, JsonObject jsonMessage) throws IOException {
        if (!jsonMessage.has("sdpOffer")) {
            log.error("❌ handleStart 오류: sdpOffer 필드 없음");
            return;
        }

        String sdpOffer = jsonMessage.get("sdpOffer").getAsString();
        kurentoManager.processSdpOffer(roomId, userId, sdpOffer, sdpAnswer -> {
            try {
                // ✅ SDP Answer 전송
                JsonObject response = new JsonObject();
                response.addProperty("id", "response");
                response.addProperty("userId", userId);
                response.addProperty("sdpAnswer", sdpAnswer);
                session.sendMessage(new TextMessage(response.toString()));

                // ✅ 채널 내 모든 클라이언트에게 최신 유저 정보 전송
                broadcastUsersInChannel(roomId);

            } catch (IOException e) {
                log.error("❌ SDP 응답 전송 실패", e);
            }
        });
    }

    /**
     * 클라이언트가 전송한 ICE Candidate를 처리
     */
    private void handleIceCandidate(String roomId, String userId, JsonObject jsonMessage) {
        if (!jsonMessage.has("candidate")) {
            log.error("❌ ICE Candidate 정보 없음: {}", jsonMessage);
            return;
        }

        IceCandidate candidate = new Gson().fromJson(jsonMessage.get("candidate"), IceCandidate.class);
        kurentoManager.sendIceCandidates(roomId, userId, candidate);
    }

    /**
     * 사용자의 오디오/비디오/화면 공유 상태를 변경하고, 모든 클라이언트에게 업데이트된 유저 목록 전송
     */
    private void toggleMediaState(String roomId, String userId, String type, boolean enabled) {
        log.info("🔄 {} 공유 상태 변경: {} - {}", type, userId, enabled);
        kurentoManager.updateUserMediaState(roomId, userId, type, enabled);

        // ✅ 변경된 유저 정보를 모든 클라이언트에게 전송
        broadcastUsersInChannel(roomId);
    }

    /**
     * 특정 채널 내 모든 클라이언트에게 업데이트된 유저 정보를 전송
     */
    private void broadcastUsersInChannel(String roomId) {
        List<GetUsersInChannelResponse.UserInRoom> users = kurentoManager.getUsersInChannel(roomId);
        GetUsersInChannelResponse response = GetUsersInChannelResponse.builder()
                .channelId(roomId)
                .users(users)
                .build();

        String jsonResponse = new Gson().toJson(response);

        for (WebSocketSession session : sessions.values()) {
            try {
                session.sendMessage(new TextMessage(jsonResponse));
            } catch (IOException e) {
                log.error("❌ WebSocket 메시지 전송 실패: {}", e.getMessage());
            }
        }
        log.info("📡 [Kurento] 모든 클라이언트에게 채널 유저 정보 전송: {}", roomId);
    }

    /**
     * 클라이언트가 WebSocket 연결을 종료하면, 세션에서 제거
     */
    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        log.info("❌ WebSocket 연결 종료: {}", session.getId());
        sessions.remove(session.getId());
    }
}