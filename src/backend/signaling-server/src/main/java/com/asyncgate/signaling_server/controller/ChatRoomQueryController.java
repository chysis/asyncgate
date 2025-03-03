package com.asyncgate.signaling_server.controller;

import com.asyncgate.signaling_server.dto.response.GetICEUrlResponse;
import com.asyncgate.signaling_server.signaling.ICEManager;
import com.asyncgate.signaling_server.support.response.SuccessResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/room")
@RequiredArgsConstructor
public class ChatRoomQueryController {

    private final ICEManager iceManager;
    /**
     * turn 서버 url 조회 get method
     */
    @GetMapping("/turn")
    public SuccessResponse<GetICEUrlResponse> getTurnServerUrl() {
        return SuccessResponse.ok(iceManager.getTurnUrl());
    }

    /**
     * sturn 서버 url 조회 get method
     */
    @GetMapping("/sturn")
    public SuccessResponse<GetICEUrlResponse> getSTurnServerUrl() {
        return SuccessResponse.ok(iceManager.getSTurnUrl());
    }
}
