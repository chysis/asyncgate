package com.asyncgate.signaling_server.infrastructure.client;

import com.asyncgate.signaling_server.domain.Member;
import com.asyncgate.signaling_server.exception.FailType;
import com.asyncgate.signaling_server.exception.SignalingServerException;
import com.asyncgate.signaling_server.infrastructure.dto.response.ReadUserRoomProfileResponse;
import com.asyncgate.signaling_server.infrastructure.utility.HttpClientUtil;
import com.asyncgate.signaling_server.support.response.SuccessResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

import static org.kurento.jsonrpc.client.JsonRpcClient.log;

@Service
@RequiredArgsConstructor
public class MemberServiceClient {

    @Value("${service.member.url}")  // ✅ application.yml에서 URL 가져오기
    private String memberServiceUrl;

    public Member fetchMemberById(String userId, String roomId) {
        Map<String, String> queryParams = new HashMap<>();
        queryParams.put("userId", userId);

        log.info("✅ 유저가 입장 요청함 - userId: {}, roomId: {}", userId, roomId);

        try {
            // ✅ HTTP 요청 실행
            SuccessResponse<ReadUserRoomProfileResponse> response = HttpClientUtil.get(
                    memberServiceUrl, "/room/profile", queryParams, SuccessResponse.class);

            // ✅ 응답이 null이거나 결과가 없을 경우 예외 처리
            if (response == null || response.getResult() == null) {
                log.warn("❌ 유저 정보 조회 실패: userId={}, roomId={}", userId, roomId);
                throw new SignalingServerException(FailType._MEMBER_NOT_FOUND);
            }

            ReadUserRoomProfileResponse userProfile = response.getResult();

            // ✅ 정상적으로 Member 객체 생성 및 반환
            return Member.create(userId, roomId, userProfile.getProfileImageUrl(), userProfile.getNickname());

        } catch (Exception e) {
            log.error("❌ MemberServiceClient 오류: 유저 정보 조회 실패 (userId={}, roomId={}, message={})", userId, roomId, e.getMessage());
            throw new SignalingServerException(FailType._UNKNOWN_ERROR);
        }
    }
}