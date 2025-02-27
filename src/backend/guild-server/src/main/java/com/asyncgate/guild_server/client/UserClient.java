package com.asyncgate.guild_server.client;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.List;

@FeignClient(name = "user-server")
public interface UserClient {

    @GetMapping("/members")
    List<UserClientInfoResponse> getUsersInfo(@RequestParam(required = false) List<String> memberIds);
}
