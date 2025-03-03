package com.asyncgate.signaling_server.signaling;

import com.asyncgate.signaling_server.config.WebRTCConfig;
import com.asyncgate.signaling_server.dto.response.GetICEUrlResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class ICEManager {
    private final WebRTCConfig webRTCConfig;

    /**
     * turn server url 조회
     */
    public GetICEUrlResponse getTurnUrl() {
        return GetICEUrlResponse.builder()
                .url(webRTCConfig.getTurnUrl())
                .build();
    }

    /**
     * sturn server url 조회
     */
    public GetICEUrlResponse getSTurnUrl() {
        return GetICEUrlResponse.builder()
                .url(webRTCConfig.getStunUrl())
                .build();
    }
}
