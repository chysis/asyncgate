package com.asyncgate.signaling_server.usecase;

import com.asyncgate.signaling_server.support.annotation.UseCase;

@UseCase
public interface ExitRoomUseCase {

    void execute(final String roomId, final String memberId);
}
