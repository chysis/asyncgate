package com.asyncgate.signaling_server.signaling;

import com.asyncgate.signaling_server.domain.Member;
import com.asyncgate.signaling_server.dto.response.GetUsersInChannelResponse;
import com.asyncgate.signaling_server.entity.MemberEntity;
import com.asyncgate.signaling_server.support.utility.DomainUtil;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.kurento.client.*;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class KurentoManager {
    private final KurentoClient kurentoClient;

    // kurento media pipline (SFU) 방에 대한 데이터 (key, value)
    private final Map<String, MediaPipeline> pipelines = new ConcurrentHashMap<>();
    private final Map<String, Map<String, WebRtcEndpoint>> roomEndpoints = new ConcurrentHashMap<>();
    private final Map<String, Member> userStates = new ConcurrentHashMap<>();

    public KurentoManager(KurentoClient kurentoClient) {
        this.kurentoClient = kurentoClient;
    }

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
    public synchronized WebRtcEndpoint createEndpoint(String roomId, String userId) {
        MediaPipeline pipeline = getOrCreatePipeline(roomId);
        WebRtcEndpoint endpoint = new WebRtcEndpoint.Builder(pipeline).build();

        // ICE Candidate 리스너 추가
        endpoint.addIceCandidateFoundListener(event -> {
            JsonObject candidateMessage = new JsonObject();
            candidateMessage.addProperty("id", "iceCandidate");
            candidateMessage.addProperty("userId", userId);
            candidateMessage.add("candidate", new Gson().toJsonTree(event.getCandidate()));

            log.info("🧊 [Kurento] ICE Candidate 전송: roomId={}, userId={}, candidate={}", roomId, userId, event.getCandidate());
        });

        // 사용자 엔드포인트 저장
        roomEndpoints.computeIfAbsent(roomId, k -> new ConcurrentHashMap<>()).put(userId, endpoint);
        userStates.put(userId, DomainUtil.MemberMapper.toDomain(
                MemberEntity.builder()
                        .userId(userId)
                        .roomId(roomId)
                        .build())
        );

        log.info("[Kurento] WebRTC Endpoint 생성: roomId={}, userId={}", roomId, userId);
        return endpoint;
    }

    /**
     * SDP Offer를 처리하고 Answer를 반환
     */
    public void processSdpOffer(String roomId, String userId, String sdpOffer, Consumer<String> callback) {
        WebRtcEndpoint endpoint = createEndpoint(roomId, userId);
        String sdpAnswer = endpoint.processOffer(sdpOffer);
        endpoint.gatherCandidates(); // ICE Candidate 수집 시작

        log.info("📡 [Kurento] SDP Offer 처리 완료: roomId={}, userId={}", roomId, userId);
        callback.accept(sdpAnswer);
    }

    /**
     * ICE Candidate를 특정 유저에게 추가
     */
    public void sendIceCandidates(String roomId, String userId, IceCandidate candidate) {
        if (!roomEndpoints.containsKey(roomId) || !roomEndpoints.get(roomId).containsKey(userId)) {
            log.error("❌ [Kurento] ICE Candidate 오류: 존재하지 않는 방 또는 사용자 (roomId={}, userId={})", roomId, userId);
            return;
        }

        WebRtcEndpoint endpoint = roomEndpoints.get(roomId).get(userId);
        endpoint.addIceCandidate(candidate);
        log.info("🧊 [Kurento] ICE Candidate 추가 완료: roomId={}, userId={}, candidate={}", roomId, userId, candidate);
    }

    /**
     * 특정 방의 모든 유저 목록 반환
     */
    public List<GetUsersInChannelResponse.UserInRoom> getUsersInChannel(String channelId) {
        if (!roomEndpoints.containsKey(channelId)) {
            log.warn("🚨 [Kurento] 조회 실패: 존재하지 않는 채널 (channelId={})", channelId);
            return Collections.emptyList();
        }

        return roomEndpoints.get(channelId).entrySet().stream()
                .map(entry -> {
                    String userId = entry.getKey();
                    WebRtcEndpoint endpoint = entry.getValue();

                    boolean isMicEnabled = endpoint.isMediaFlowingIn(MediaType.AUDIO) && endpoint.isMediaFlowingOut(MediaType.AUDIO);
                    boolean isCameraEnabled = endpoint.isMediaFlowingIn(MediaType.VIDEO) && endpoint.isMediaFlowingOut(MediaType.VIDEO);

                    return GetUsersInChannelResponse.UserInRoom.builder()
                            .id(userId)
                            .nickname(userId)  // 닉네임 정보가 없으면 기본 userId 사용
                            .profileImage("")  // 프로필 이미지 필드가 없으면 기본 값 설정
                            .isMicEnabled(isMicEnabled)
                            .isCameraEnabled(isCameraEnabled)
                            .isScreenSharingEnabled(false)
                            .build();
                })
                .collect(Collectors.toList());
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
     * 특정 유저의 미디어 상태 (음성, 영상, 화면 공유) 변경
     */
    public void updateUserMediaState(String roomId, String userId, String type, boolean enabled) {
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
            case "audio":
                if (enabled) {
                    reconnectAudio(userId, endpoint);
                } else {
                    disconnectAudio(userId, endpoint);
                }
                log.info("🔊 [Kurento] Audio 상태 변경: roomId={}, userId={}, enabled={}", roomId, userId, enabled);
                member.updateMediaState("mic", enabled);
                break;

            case "video":
                if (enabled) {
                    reconnectVideo(userId, endpoint);
                } else {
                    disconnectVideo(userId, endpoint);
                }
                log.info("📹 [Kurento] Video 상태 변경: roomId={}, userId={}, enabled={}", roomId, userId, enabled);
                member.updateMediaState("camera", enabled);
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
            userStates.get(userId).updateMediaState("mic", false);
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
            userStates.get(userId).updateMediaState("mic", true);
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
            userStates.get(userId).updateMediaState("camera", false);
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
            userStates.get(userId).updateMediaState("camera", true);
        }
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
    }
}