package com.asyncgate.signaling_server.support.handler;

import com.asyncgate.signaling_server.dto.response.GetUsersInChannelResponse;
import com.asyncgate.signaling_server.security.constant.Constants;
import com.asyncgate.signaling_server.security.utility.JsonWebTokenUtil;
import com.asyncgate.signaling_server.signaling.KurentoManager;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import io.jsonwebtoken.Claims;
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

    private final JsonWebTokenUtil jsonWebTokenUtil;

    public KurentoHandler(KurentoManager kurentoManager, JsonWebTokenUtil jsonWebTokenUtil) {
        this.kurentoManager = kurentoManager;
        this.jsonWebTokenUtil = jsonWebTokenUtil;
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

        // jwt token 검증
        if (!jsonMessage.has("token")) {
            log.error("❌ WebSocket 메시지 오류: token 필드 없음");
            return;
        }

        String token = jsonMessage.get("token").getAsString();
        Claims claims = jsonWebTokenUtil.validate(token);

        String memberId = claims.get(Constants.MEMBER_ID_CLAIM_NAME, String.class);

        log.info("🔑 JWT 인증 성공 - userId: {}", memberId);

        if (!jsonMessage.has("type")) {
            log.error("❌ WebSocket 메시지 오류: type 필드 없음");
            return;
        }

        if (!jsonMessage.has("data") || !jsonMessage.getAsJsonObject("data").has("roomId")) {
            log.error("❌ WebSocket 메시지 오류: data.roomId 필드 없음");
            return;
        }

        String messageType = jsonMessage.get("type").getAsString();
        JsonObject data = jsonMessage.getAsJsonObject("data");
        String roomId = data.get("roomId").getAsString();  // roomId 접근

        System.out.println("roomId: " + roomId);

        switch (messageType) {
            case "getUsers":
                sendUsersInChannel(session, roomId);
                break;
            case "offer":
                handleJoin(session, roomId, memberId, jsonMessage);
                break;
            case "candidate":
                handleIceCandidate(roomId, memberId, jsonMessage);
                break;
            case "AUDIO":
                toggleMediaState(roomId, memberId, "AUDIO", jsonMessage.get("enabled").getAsBoolean());
                break;
            case "MEDIA":
                toggleMediaState(roomId, memberId, "MEDIA", jsonMessage.get("enabled").getAsBoolean());
                break;
            case "DATA":
                toggleMediaState(roomId, memberId, "DATA", jsonMessage.get("enabled").getAsBoolean());
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
    /**
     * 사용자가 WebRTC 연결을 시작할 때 처리 (SDP Offer → SDP Answer 반환)
     */
    private void handleJoin(WebSocketSession session, String roomId, String userId, JsonObject jsonMessage) throws IOException {

        // 있으면 데이터, 없으면 빈 문자열
        JsonObject data = jsonMessage.has("data") ? jsonMessage.getAsJsonObject("data") : null;
        String sdpOffer = (data != null && data.has("sdpOffer")) ? data.get("sdpOffer").getAsString() : null;

        kurentoManager.processSdpOffer(roomId, userId, sdpOffer, sdpAnswer -> {
            try {
                // ✅ 현재 방에 있는 모든 유저 정보 가져오기
                List<GetUsersInChannelResponse.UserInRoom> users = kurentoManager.getUsersInChannel(roomId);

                // ✅ SDP Answer 및 유저 상태 정보를 함께 전송
                JsonObject response = new JsonObject();
                response.addProperty("id", "response");
                response.addProperty("user_id", userId);
                // ✅ sdpAnswer가 null이면 빈 문자열("")로 처리
                response.addProperty("sdpAnswer", sdpAnswer != null ? sdpAnswer : "");

                // ✅ 유저 상태 정보 추가 (음성, 화상, 화면 공유 상태 포함)
                JsonArray usersArray = new JsonArray();
                for (GetUsersInChannelResponse.UserInRoom user : users) {
                    JsonObject userJson = new JsonObject();
                    userJson.addProperty("id", user.getId());
                    userJson.addProperty("nickname", user.getNickname());
                    userJson.addProperty("profile_image_url", user.getProfileImage());
                    userJson.addProperty("audio", user.isMicEnabled());
                    userJson.addProperty("video", user.isCameraEnabled());
                    userJson.addProperty("screen", user.isScreenSharingEnabled());
                    usersArray.add(userJson);
                }
                response.add("users", usersArray);

                // ✅ SDP Answer + 유저 정보 함께 전송
                session.sendMessage(new TextMessage(response.toString()));

                log.info("📡 [Kurento] SDP Answer 및 유저 정보 전송 - roomId: {}", roomId);

            } catch (IOException e) {
                log.error("❌ SDP 응답 전송 실패", e);
            }
        });
    }

    /**
     * 클라이언트가 전송한 ICE Candidate를 처리
     */
    private void handleIceCandidate(String roomId, String userId, JsonObject jsonMessage) {
        if (!jsonMessage.has("data") || !jsonMessage.getAsJsonObject("data").has("candidate")) {
            log.error("❌ ICE Candidate 정보 없음: {}", jsonMessage);
            return;
        }

        JsonObject data = jsonMessage.getAsJsonObject("data");
        String extractedRoomId = data.has("roomId") ? data.get("roomId").getAsString() : null;

        if (extractedRoomId == null) {
            log.error("❌ ICE Candidate 오류: roomId 필드 없음");
            return;
        }

        IceCandidate candidate = new Gson().fromJson(data.get("candidate"), IceCandidate.class);
        kurentoManager.sendIceCandidates(extractedRoomId, userId, candidate);
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