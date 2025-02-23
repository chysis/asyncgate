package com.asyncgate.signaling_server.signaling;

import com.asyncgate.signaling_server.domain.Member;
import com.asyncgate.signaling_server.dto.response.GetUsersInChannelResponse;
import com.asyncgate.signaling_server.entity.MemberEntity;
import com.asyncgate.signaling_server.support.utility.DomainUtil;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.kurento.client.IceCandidate;
import org.kurento.client.KurentoClient;
import org.kurento.client.MediaPipeline;
import org.kurento.client.WebRtcEndpoint;
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
            log.info("🎥 [Kurento] 새로운 MediaPipeline 생성: {}", roomId);
            return kurentoClient.createMediaPipeline();
        });
    }

    /**
     * WebRTC 엔드포인트 생성 및 ICE Candidate 리스너 설정
     */
    public synchronized WebRtcEndpoint createEndpoint(String roomId, String userId) {
        MediaPipeline pipeline = getOrCreatePipeline(roomId);
        WebRtcEndpoint endpoint = new WebRtcEndpoint.Builder(pipeline).build();

        endpoint.addIceCandidateFoundListener(event -> {
            JsonObject candidateMessage = new JsonObject();
            candidateMessage.addProperty("id", "iceCandidate");
            candidateMessage.addProperty("userId", userId);
            candidateMessage.add("candidate", new Gson().toJsonTree(event.getCandidate()));

            log.info("🧊 [Kurento] ICE Candidate 전송: roomId={}, userId={}, candidate={}", roomId, userId, event.getCandidate());
        });

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
     * 특정 방에서 유저를 제거하고, 필요시 방 정리
     */
    public void removeUser(String roomId, String userId) {
        if (roomEndpoints.containsKey(roomId)) {
            WebRtcEndpoint endpoint = roomEndpoints.get(roomId).remove(userId);
            if (endpoint != null) {
                endpoint.release();
            }

            if (roomEndpoints.get(roomId).isEmpty()) {
                roomEndpoints.remove(roomId);
                MediaPipeline pipeline = pipelines.remove(roomId);
                if (pipeline != null) {
                    pipeline.release();
                }
            }
        }

        userStates.remove(userId);
        log.info("🚪 [Kurento] 유저 제거 완료: roomId={}, userId={}", roomId, userId);
    }

    /**
     * 특정 방의 모든 유저 목록 반환
     */
    public List<GetUsersInChannelResponse.UserInRoom> getUsersInChannel(String channelId) {
        if (!roomEndpoints.containsKey(channelId)) {
            log.warn("🚨 [Kurento] 조회 실패: 존재하지 않는 채널 (channelId={})", channelId);
            return Collections.emptyList();
        }

        return userStates.entrySet().stream()
                .filter(entry -> channelId.equals(entry.getValue().getRoomId()))
                .map(entry -> {
                    Member member = entry.getValue();
                    return GetUsersInChannelResponse.UserInRoom.builder()
                            .id(member.getUserId())
                            .nickname(member.getUserId())  // 닉네임 정보가 없으면 기본 userId 사용
                            .profileImage("")  // 프로필 이미지 필드가 없으면 기본 값 설정
                            .isMicEnabled(member.isMicEnabled())
                            .isCameraEnabled(member.isCameraEnabled())
                            .isScreenSharingEnabled(member.isScreenSharingEnabled())
                            .build();
                })
                .collect(Collectors.toList());
    }

    /**
     * 유저가 방에 접속함.
     */
    public void addUserToRoom(String roomId, String userId) {
        userStates.put(userId, DomainUtil.MemberMapper.toDomain(
                MemberEntity.builder()
                        .userId(userId)
                        .roomId(roomId)
                        .build())
        );
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
        switch (type) {
            case "audio":
                log.info("🔊 [Kurento] Audio 상태 변경: roomId={}, userId={}, enabled={}", roomId, userId, enabled);
                member.updateMediaState("mic", enabled);
                break;
            case "video":
                log.info("📹 [Kurento] Video 상태 변경: roomId={}, userId={}, enabled={}", roomId, userId, enabled);
                member.updateMediaState("camera", enabled);
                break;
            case "screen":
                log.info("🖥️ [Kurento] Screen 상태 변경: roomId={}, userId={}, enabled={}", roomId, userId, enabled);
                member.updateMediaState("screen", enabled);
                break;
            default:
                log.warn("⚠️ [Kurento] 잘못된 미디어 타입: {}", type);
                return;
        }
    }
}