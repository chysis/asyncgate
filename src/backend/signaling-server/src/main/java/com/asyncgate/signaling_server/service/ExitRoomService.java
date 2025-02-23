package com.asyncgate.signaling_server.service;

import com.asyncgate.signaling_server.signaling.KurentoManager;
import com.asyncgate.signaling_server.usecase.ExitRoomUseCase;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class ExitRoomService implements ExitRoomUseCase {
    private final KurentoManager kurentoManager;

    /**
     * 방에 유저 추가
     */
    @Override
    public void execute(final String roomId, final String memberId) {
        kurentoManager.removeUser(roomId, memberId);
    }
}
