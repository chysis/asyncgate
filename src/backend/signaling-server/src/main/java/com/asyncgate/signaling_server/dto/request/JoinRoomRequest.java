package com.asyncgate.signaling_server.dto.request;

import com.fasterxml.jackson.annotation.JsonProperty;

public record JoinRoomRequest(

        @JsonProperty("audio_enabled")
        boolean audioEnabled,

        @JsonProperty("media_enabled")
        boolean mediaEnabled,

        @JsonProperty("data_enabled")
        boolean dataEnabled
) {
}
