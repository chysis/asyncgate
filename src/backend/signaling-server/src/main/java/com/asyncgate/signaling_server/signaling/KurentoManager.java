package com.asyncgate.signaling_server.signaling;

import com.asyncgate.signaling_server.domain.Member;
import com.asyncgate.signaling_server.dto.response.GetUsersInChannelResponse;
import com.asyncgate.signaling_server.exception.FailType;
import com.asyncgate.signaling_server.exception.SignalingServerException;
import com.asyncgate.signaling_server.infrastructure.client.MemberServiceClient;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import lombok.RequiredArgsConstructor;
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
    public synchronized WebRtcEndpoint createEndpoint(String roomId, String userId) {
        MediaPipeline pipeline = getOrCreatePipeline(roomId);
        WebRtcEndpoint endpoint = new WebRtcEndpoint.Builder(pipeline).build();

        // 사용자 정보 조회 (비동기)
        memberServiceClient.fetchMemberById(userId, roomId)
                .doOnSuccess(member -> {
                    log.info("✔ 성공적으로 사용자 정보 조회: {}", member);

                    // ICE Candidate 리스너 추가
                    endpoint.addIceCandidateFoundListener(event -> {
                        JsonObject candidateMessage = new JsonObject();
                        candidateMessage.addProperty("id", "iceCandidate");
                        candidateMessage.addProperty("userId", member.getId());
                        candidateMessage.add("candidate", new Gson().toJsonTree(event.getCandidate()));

                        log.info("🧊 [Kurento] ICE Candidate 전송: roomId={}, userId={}, candidate={}", roomId, member.getId(), event.getCandidate());
                    });

                    // 사용자 엔드포인트 저장
                    // 음성, 화상용
                    roomEndpoints.computeIfAbsent(roomId, k -> new ConcurrentHashMap<>()).put(userId, endpoint);

                    // 유저 데이터 저장
                    userStates.put(userId, member);
                })
                .doOnError(error -> log.error("❌ Member 정보 조회 실패: roomId={}, userId={}, message={}", roomId, userId, error.getMessage()))
                .subscribe();

        log.info("[Kurento] WebRTC Endpoint 생성: roomId={}, userId={}", roomId, userId);
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

        // SDP Offer 처리 및 SDP Answer 생성
        String sdpAnswer = endpoint.processOffer(sdpOffer);

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
     * ICE Candidate를 특정 유저에게 추가
     */
    public void sendIceCandidates(String roomId, String userId, IceCandidate candidate) {

        WebRtcEndpoint endpoint = getUserEndpoint(roomId, userId);

        if (endpoint == null) {
            log.error("❌ [Kurento] WebRTC Endpoint 없음: roomId={}, userId={}", roomId, userId);
            return;
        }

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


                    Member member = userStates.get(userId);

                    System.out.println("member를 찾았습니다. : " + member.getId());

                    // member가 null이라면 exception
                    if (member == null) {
                        throw new SignalingServerException(FailType._MEMBER_NOT_FOUND);
                    }

                    boolean isMicEnabled = endpoint.isMediaFlowingIn(MediaType.AUDIO) && endpoint.isMediaFlowingOut(MediaType.AUDIO);
                    boolean isCameraEnabled = endpoint.isMediaFlowingIn(MediaType.VIDEO) && endpoint.isMediaFlowingOut(MediaType.VIDEO);

                    return GetUsersInChannelResponse.UserInRoom.builder()
                            .id(member.getId())
                            .nickname(member.getNickname())  // 닉네임 정보가 없으면 기본 userId 사용
                            .profileImage(member.getProgileImageUrl())  // 프로필 이미지 필드가 없으면 기본 값 설정
                            .isMicEnabled(isMicEnabled)
                            .isCameraEnabled(isCameraEnabled)
                            .isScreenSharingEnabled(false)
                            .build();
                })
                .collect(Collectors.toList());
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
            case "AUDIO":
                if (enabled) {
                    reconnectAudio(userId, endpoint);
                } else {
                    disconnectAudio(userId, endpoint);
                }
                log.info("🔊 [Kurento] Audio 상태 변경: roomId={}, userId={}, enabled={}", roomId, userId, enabled);
                member.updateMediaState("AUDIO", enabled);
                break;

            case "VIDEO":
                if (enabled) {
                    reconnectVideo(userId, endpoint);
                } else {
                    disconnectVideo(userId, endpoint);
                }
                log.info("📹 [Kurento] Video 상태 변경: roomId={}, userId={}, enabled={}", roomId, userId, enabled);
                member.updateMediaState("VIDEO", enabled);
                break;

                // 화면공유
            case "DATA":
                if (enabled) {
                    reconnectScreenShare(userId, endpoint);
                } else {
                    disconnectScreenShare(userId, endpoint);
                }
                log.info("🖥️ [Kurento] ScreenShare 상태 변경: roomId={}, userId={}, enabled={}", roomId, userId, enabled);
                member.updateMediaState("DATA", enabled);
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
     * 특정 사용자의 화면 공유 스트림 다시 연결
     */
    private void reconnectScreenShare(String userId, WebRtcEndpoint endpoint) {
        endpoint.connect(endpoint, MediaType.DATA);
        log.info("🖥️ [Kurento] 화면 공유 활성화: userId={}", userId);

        // ✅ userStates에서 해당 사용자의 상태 업데이트
        if (userStates.containsKey(userId)) {
            userStates.get(userId).updateMediaState("screenShare", true);
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
            userStates.get(userId).updateMediaState("screenShare", false);
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

        log.info("🛑 [Kurento] 방 제거 완료: roomId={}", roomId);
    }
}