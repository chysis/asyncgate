package com.asyncgate.signaling_server.domain;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class Member implements Identifiable {

    private final String id; // 고유한 식별자
    private final String progileImageUrl;
    private final String nickname;
    private String roomId; // 사용자가 속한 방 ID

    // 미디어 상태
    private boolean isMicEnabled;
    private boolean isCameraEnabled;
    private boolean isScreenSharingEnabled;

    @Builder
    public Member(final String id, final String roomId, final String progileImageUrl, final String nickname, final boolean isMicEnabled, final boolean isCameraEnabled, final boolean isScreenSharingEnabled) {
        this.id = id;
        this.roomId = roomId;
        this.progileImageUrl = progileImageUrl;
        this.nickname = nickname;
        this.isMicEnabled = isMicEnabled;
        this.isCameraEnabled = isCameraEnabled;
        this.isScreenSharingEnabled = isScreenSharingEnabled;
    }

    public static Member create(final String id, final String roomId, final String progileImageUrl, final String nickname, final boolean isMicEnabled, final boolean isCameraEnabled, final boolean isScreenSharingEnabled) {
        return Member.builder()
                .id(id)
                .roomId(roomId)
                .progileImageUrl(progileImageUrl)
                .nickname(nickname)
                .isMicEnabled(isMicEnabled)
                .isCameraEnabled(isCameraEnabled)
                .isScreenSharingEnabled(isScreenSharingEnabled)
                .build();
    }

    // 미디어 상태 업데이트
    public void updateMediaState(String type, boolean enabled) {
        switch (type) {
            case "AUDIO":
                this.isMicEnabled = enabled;
                break;
            case "MEDIA":
                this.isCameraEnabled = enabled;
                break;
            case "DATA":
                this.isScreenSharingEnabled = enabled;
                break;
            default:
                throw new IllegalArgumentException("잘못된 미디어 타입: " + type);
        }
    }
}