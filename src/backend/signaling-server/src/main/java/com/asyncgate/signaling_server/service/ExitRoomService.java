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

    @Override
    public void execute(final String roomId, final String memberId) {
        log.info("ExitRoomService.execute 호출됨, roomId: {}, memberId: {}", roomId, memberId);

        // 방에서 유저 제거
        kurentoManager.removeUser(roomId, memberId);
    }
}
