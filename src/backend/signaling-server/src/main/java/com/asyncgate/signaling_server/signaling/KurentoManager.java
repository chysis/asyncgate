package com.asyncgate.signaling_server.signaling;

import com.asyncgate.signaling_server.domain.Member;
import com.asyncgate.signaling_server.dto.request.JoinRoomRequest;
import com.asyncgate.signaling_server.dto.response.GetUsersInChannelResponse;
import com.asyncgate.signaling_server.entity.type.MemberMediaType;
import com.asyncgate.signaling_server.infrastructure.client.MemberServiceClient;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import lombok.RequiredArgsConstructor;
import org.kurento.client.*;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

@Slf4j
@Service
@RequiredArgsConstructor
public class KurentoManager {
    private final KurentoClient kurentoClient;

    private final MemberServiceClient memberServiceClient;

    // kurento media pipline (SFU) 방에 대한 데이터 (key, value)
    private final Map<String, MediaPipeline> pipelines = new ConcurrentHashMap<>();
    private final Map<String, Map<String, WebRtcEndpoint>> roomEndpoints = new ConcurrentHashMap<>();

    private final Map<String, Member> userStates = new ConcurrentHashMap<>();

    /**
     * 특정 방에 대한 MediaPipeline을 가져오거나 새로 생성
     */
    public synchronized MediaPipeline getOrCreatePipeline(String roomId) {
        return pipelines.computeIfAbsent(roomId, id -> {
            return kurentoClient.createMediaPipeline();
        });
    }

    /**
     * WebRTC 엔드포인트 생성 및 ICE Candidate 리스너 설정
     */
    public synchronized WebRtcEndpoint getOrCreateEndpoint(String roomId, String userId, JoinRoomRequest request) {
        MediaPipeline pipeline = getOrCreatePipeline(roomId);

        // WebRtcEndpoint 가져오기 또는 생성
        WebRtcEndpoint endpoint = roomEndpoints
                .computeIfAbsent(roomId, k -> new ConcurrentHashMap<>())
                .computeIfAbsent(userId, k -> new WebRtcEndpoint.Builder(pipeline).build());

        log.info("🛠 WebRTC Endpoint 생성 또는 가져오기 완료: roomId={}, userId={}", roomId, userId);

        try {
            // 동기적으로 사용자 정보 가져오기
            Member member = memberServiceClient.fetchMemberById(userId, roomId, request).block(Duration.ofSeconds(5));

            if (member != null) {
                log.info("✔ 성공적으로 사용자 정보 조회: {}", member);

                // ICE Candidate 리스너 추가
                endpoint.addIceCandidateFoundListener(event -> {
                    JsonObject candidateMessage = new JsonObject();
                    candidateMessage.addProperty("id", "iceCandidate");
                    candidateMessage.addProperty("userId", member.getId());
                    candidateMessage.add("candidate", new Gson().toJsonTree(event.getCandidate()));

                    log.info("🧊 ICE Candidate 전송: roomId={}, userId={}, candidate={}",
                            roomId, member.getId(), event.getCandidate());
                });

                // 사용자 엔드포인트 저장 (음성, 화상용)
                roomEndpoints.computeIfAbsent(roomId, k -> new ConcurrentHashMap<>()).put(userId, endpoint);

                // 유저 데이터 저장
                userStates.put(userId, member);

                log.info("✅ 사용자 데이터 및 엔드포인트 저장 완료: roomId={}, userId={}", roomId, userId);
            } else {
                log.warn("⚠ 사용자 정보를 찾을 수 없음: roomId={}, userId={}", roomId, userId);
            }
        } catch (Exception e) {
            log.error("❌ Member 정보 조회 실패 (동기 처리): roomId={}, userId={}, message={}",
                    roomId, userId, e.getMessage());
        }

        return endpoint;
    }

    // 특정 유저의 endpoint 찾기
    public WebRtcEndpoint getUserEndpoint(String roomId, String userId) {
        if (!roomEndpoints.containsKey(roomId) || !roomEndpoints.get(roomId).containsKey(userId)) {
            log.error("❌ [Kurento] WebRTC Endpoint 없음: roomId={}, userId={}", roomId, userId);
            return null;
        }

        return roomEndpoints.get(roomId).get(userId);
    }

    /**
     * SDP Offer를 처리하고 Answer를 반환
     */
    public void processSdpOffer(String roomId, String userId, String sdpOffer, Consumer<String> callback) {
        WebRtcEndpoint endpoint = getUserEndpoint(roomId, userId);

        System.out.println("processSdpOffer 접근함, endpoint get 성공" + endpoint);

        if (endpoint.getMediaState() == MediaState.CONNECTED) {
            log.warn("⚠️ 이미 SDP 협상이 완료된 상태입니다. 새로운 Offer를 처리하지 않습니다.");
            return;
        }

        // SDP Offer 처리 및 SDP Answer 생성
        String sdpAnswer = endpoint.processOffer(sdpOffer);

        System.out.println("sdp 처리 및 sdp answer 생성" + sdpAnswer);

        // ICE Candidate 수집
        endpoint.gatherCandidates();

        log.info("📡 [Kurento] SDP Offer 처리 완료: roomId={}, userId={}", roomId, userId);

        // SDP Answer 반환
        callback.accept(sdpAnswer);
    }

    // SDP Offer & Answer 조회
    public String getSdpOffer(String roomId, String userId) {
        WebRtcEndpoint endpoint = getUserEndpoint(roomId, userId);
        return endpoint != null ? endpoint.getLocalSessionDescriptor() : null;
    }

    public String getSdpAnswer(String roomId, String userId) {
        WebRtcEndpoint endpoint = getUserEndpoint(roomId, userId);
        return endpoint != null ? endpoint.getRemoteSessionDescriptor() : null;
    }

    /**
     * ICE Candidate를 특정 유저에게 추가하고 같은 방의 모든 클라이언트에게 전송
     */
    public void sendIceCandidates(WebSocketSession session, String roomId, String userId, IceCandidate candidate) {
        WebRtcEndpoint endpoint = getUserEndpoint(roomId, userId);

        if (endpoint == null) {
            log.error("❌ [Kurento] WebRTC Endpoint 없음: roomId={}, userId={}", roomId, userId);
            return;
        }

        // ICE Candidate 추가
        endpoint.addIceCandidate(candidate);
        log.info("🧊 [Kurento] ICE Candidate 추가 완료: roomId={}, userId={}, candidate={}", roomId, userId, candidate);

        // ICE Candidate 메시지 생성
        JsonObject candidateMessage = new JsonObject();
        candidateMessage.addProperty("type", "iceCandidate");  // id → type 통일
        candidateMessage.addProperty("userId", userId);
        candidateMessage.add("candidate", new Gson().toJsonTree(candidate));

        try {
            // WebSocket을 통해 ICE Candidate 전송
            session.sendMessage(new TextMessage(candidateMessage.toString()));
            log.info("📡 [Kurento] ICE Candidate 전송 완료: roomId={}, toUserId={}", roomId, userId);
        } catch (IOException e) {
            log.error("❌ ICE Candidate 전송 실패: roomId={}, toUserId={}", roomId, userId, e);
        }
    }

    /**
     * 특정 방의 모든 유저 목록 반환
     */
    public List<GetUsersInChannelResponse.UserInRoom> getUsersInChannel(String channelId) {
        if (!roomEndpoints.containsKey(channelId)) {
            log.warn("🚨 [Kurento] 조회 실패: 존재하지 않는 채널 (channelId={})", channelId);
            return Collections.emptyList();
        }

        log.info("📡 [Kurento] userStates 현재 상태: {}", userStates);
        userStates.forEach((key, value) -> log.info("🔍 userId={}, member={}", key, value));

        return roomEndpoints.get(channelId).keySet().stream()
                .map(userId -> {
                    Member member = userStates.get(userId);
                    if (member == null) {
                        log.warn("⚠️ [Kurento] userStates에서 userId={}에 대한 멤버 정보를 찾을 수 없습니다. 건너뜁니다.", userId);
                        return null; // 🚨 `null` 반환 (filter에서 제거)
                    }

                    return GetUsersInChannelResponse.UserInRoom.builder()
                            .id(member.getId())
                            .nickname(member.getNickname())
                            .profileImage(member.getProgileImageUrl())
                            .isMicEnabled(member.isAudioEnabled())
                            .isCameraEnabled(member.isMediaEnabled())
                            .isScreenSharingEnabled(member.isDataEnabled())
                            .build();
                })
                .filter(Objects::nonNull)  // 🚀 `null`인 경우 건너뛰기
                .collect(Collectors.toList());
    }

    /**
     * 특정 유저의 미디어 상태 (음성, 영상, 화면 공유) 변경
     */
    public void updateUserMediaState(String roomId, String userId, MemberMediaType type, boolean enabled) {
        if (!userStates.containsKey(userId)) {
            log.warn("⚠️ [Kurento] 미디어 상태 업데이트 실패: 존재하지 않는 유저 (userId={})", userId);
            return;
        }

        Member member = userStates.get(userId);
        WebRtcEndpoint endpoint = getUserEndpoint(roomId, userId);

        if (endpoint == null) {
            log.warn("⚠️ [Kurento] WebRTC Endpoint 없음: roomId={}, userId={}", roomId, userId);
            return;
        }

        switch (type) {
            case AUDIO:
                if (enabled) {
                    reconnectAudio(userId, endpoint);
                } else {
                    disconnectAudio(userId, endpoint);
                }
                log.info("🔊 [Kurento] Audio 상태 변경: roomId={}, userId={}, enabled={}", roomId, userId, enabled);
                member.updateMediaState(MemberMediaType.AUDIO, enabled);
                break;

            case MEDIA:
                if (enabled) {
                    reconnectVideo(userId, endpoint);
                } else {
                    disconnectVideo(userId, endpoint);
                }
                log.info("📹 [Kurento] Video 상태 변경: roomId={}, userId={}, enabled={}", roomId, userId, enabled);
                member.updateMediaState(MemberMediaType.MEDIA, enabled);
                break;

                // 화면공유
            case DATA:
                if (enabled) {
                    reconnectScreenShare(userId, endpoint);
                } else {
                    disconnectScreenShare(userId, endpoint);
                }
                log.info("🖥️ [Kurento] ScreenShare 상태 변경: roomId={}, userId={}, enabled={}", roomId, userId, enabled);
                member.updateMediaState(MemberMediaType.DATA, enabled);
                break;

            default:
                log.warn("⚠️ [Kurento] 잘못된 미디어 타입: {}", type);
                return;
        }
    }

    /**
     * 특정 사용자의 오디오 스트림 연결 해제
     */
    private void disconnectAudio(String userId, WebRtcEndpoint endpoint) {
        endpoint.disconnect(endpoint, MediaType.AUDIO);
        log.info("🚫 [Kurento] 오디오 비활성화: userId={}", userId);

        // ✅ userStates에서 해당 사용자의 상태 업데이트
        if (userStates.containsKey(userId)) {
            userStates.get(userId).updateMediaState(MemberMediaType.AUDIO, false);
        }
    }

    /**
     * 특정 사용자의 오디오 스트림 다시 연결
     */
    private void reconnectAudio(String userId, WebRtcEndpoint endpoint) {
        endpoint.connect(endpoint, MediaType.AUDIO);
        log.info("🔊 [Kurento] 오디오 활성화: userId={}", userId);

        // ✅ userStates에서 해당 사용자의 상태 업데이트
        if (userStates.containsKey(userId)) {
            userStates.get(userId).updateMediaState(MemberMediaType.AUDIO, true);
        }
    }

    /**
     * 특정 사용자의 비디오 스트림 연결 해제
     */
    private void disconnectVideo(String userId, WebRtcEndpoint endpoint) {
        endpoint.disconnect(endpoint, MediaType.VIDEO);
        log.info("🚫 [Kurento] 비디오 비활성화: userId={}", userId);

        // ✅ userStates에서 해당 사용자의 상태 업데이트
        if (userStates.containsKey(userId)) {
            userStates.get(userId).updateMediaState(MemberMediaType.MEDIA, false);
        }
    }

    /**
     * 특정 사용자의 비디오 스트림 다시 연결
     */
    private void reconnectVideo(String userId, WebRtcEndpoint endpoint) {
        endpoint.connect(endpoint, MediaType.VIDEO);
        log.info("📹 [Kurento] 비디오 활성화: userId={}", userId);

        // ✅ userStates에서 해당 사용자의 상태 업데이트
        if (userStates.containsKey(userId)) {
            userStates.get(userId).updateMediaState(MemberMediaType.MEDIA, true);
        }
    }

    /**
     * 특정 사용자의 화면 공유 스트림 다시 연결
     */
    private void reconnectScreenShare(String userId, WebRtcEndpoint endpoint) {
        endpoint.connect(endpoint, MediaType.DATA);
        log.info("🖥️ [Kurento] 화면 공유 활성화: userId={}", userId);

        // ✅ userStates에서 해당 사용자의 상태 업데이트
        if (userStates.containsKey(userId)) {
            userStates.get(userId).updateMediaState(MemberMediaType.DATA, true);
        }
    }

    /**
     * 특정 사용자의 화면 공유 스트림 연결 해제
     */
    private void disconnectScreenShare(String userId, WebRtcEndpoint endpoint) {
        endpoint.disconnect(endpoint, MediaType.DATA);
        log.info("🚫 [Kurento] 화면 공유 비활성화: userId={}", userId);

        // ✅ userStates에서 해당 사용자의 상태 업데이트
        if (userStates.containsKey(userId)) {
            userStates.get(userId).updateMediaState(MemberMediaType.DATA, false);
        }
    }

    /**
     * 방에서 특정 사용자 제거
     */
    public void removeUserFromChannel(String roomId, String userId) {
        if (!roomEndpoints.containsKey(roomId) || !roomEndpoints.get(roomId).containsKey(userId)) {
            log.warn("⚠️ [Kurento] 사용자 제거 실패: 존재하지 않는 사용자 (roomId={}, userId={})", roomId, userId);
            return;
        }

        // WebRTC Endpoint 제거
        roomEndpoints.get(roomId).get(userId).release();
        roomEndpoints.get(roomId).remove(userId);

        // 사용자 정보 제거
        userStates.remove(userId);

        log.info("🛑 [Kurento] 사용자 제거 완료: roomId={}, userId={}", roomId, userId);
    }

    /**
     * 방을 제거함
     */
    public void removeRoom(String roomId) {
        if (roomEndpoints.containsKey(roomId)) {
            roomEndpoints.get(roomId).values().forEach(WebRtcEndpoint::release);
            roomEndpoints.remove(roomId);
        }

        if (pipelines.containsKey(roomId)) {
            pipelines.get(roomId).release();
            pipelines.remove(roomId);
        }

        log.info("🛑 [Kurento] 방 제거 완료: roomId={}", roomId);
    }
}