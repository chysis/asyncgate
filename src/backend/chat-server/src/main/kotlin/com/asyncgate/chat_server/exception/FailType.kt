package com.asyncgate.chat_server.exception

import org.springframework.http.HttpStatus

enum class FailType(
    val status: HttpStatus,
    val errorCode: String,
    val message: String,
) {
    // 예시
    JWT_INVALID_SIGNATURE(HttpStatus.UNAUTHORIZED, "Jwt_4011", "JWT 서명이 유효하지 않습니다."),
    JWT_EXPIRED_TOKEN(HttpStatus.UNAUTHORIZED, "Jwt_4012", "JWT 토큰이 만료되었습니다."),
    JWT_MALFORMED_TOKEN(HttpStatus.UNAUTHORIZED, "Jwt_4013", "잘못된 형식의 JWT 토큰입니다."),
    JWT_UNSUPPORTED_TOKEN(HttpStatus.UNAUTHORIZED, "Jwt_4014", "지원하지 않는 JWT 토큰입니다."),
    JWT_INVALID_TOKEN(HttpStatus.UNAUTHORIZED, "Jwt_4015", "유효하지 않은 JWT 토큰입니다."),

    // 알 수 없는 에
    X_UNKNOWN_ERROR(HttpStatus.INTERNAL_SERVER_ERROR, "Server_5000", "알 수 없는 에러가 발생하였습니다."),

    // S3
    X_UPLOAD_FILE_ERROR(HttpStatus.INTERNAL_SERVER_ERROR, "S3_5001", "S3 이미지 업로드에 실패하였습니다."),
    X_DELETE_FILE_ERROR(HttpStatus.INTERNAL_SERVER_ERROR, "S3_5002", "S3 이미지 제거에 실패하였습니다."),
    X_FILE_NOT_FOUND(HttpStatus.NOT_FOUND, "S3_5003", "파일이 S3에 존재하지 않습니다."),

    X_DIRECT_INTERNAL_ERROR(HttpStatus.INTERNAL_SERVER_ERROR, "DirectMessage_5001", "DirectMessage id는 null일 수 없습니다."),

    ;
}
