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

@Service
@RequiredArgsConstructor
public class MemberServiceClient {

    // @Value("${service.member.url}")
    private String memberServiceUrl = "https://api.asyncgate.site/users";

    public Member fetchMemberById(String userId, String roomId) {
        Map<String, String> queryParams = new HashMap<>();
        queryParams.put("userId", userId);

        System.out.println("유저가 들어오려고 합니다.: " + userId);
        // room 출력
        System.out.println("룸 id : " + roomId);

        SuccessResponse<ReadUserRoomProfileResponse> response = HttpClientUtil.get(
                memberServiceUrl, "/room/profile", queryParams, SuccessResponse.class);

        if (response == null || response.getResult() == null) {
            throw new SignalingServerException(FailType._MEMBER_NOT_FOUND);
        }

        ReadUserRoomProfileResponse userProfile = response.getResult();
        return Member.create(userId, roomId, userProfile.getProfileImageUrl(), userProfile.getNickname());
    }
}