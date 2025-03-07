package com.asyncgate.user_server.exception;

import lombok.Getter;
import org.springframework.http.HttpStatus;

@Getter
public enum FailType {

    // 예시
    // 400대  JWT_BAD_REQUEST  500 대 _JWT_BAD_REQUEST
    JWT_BAD_REQUEST(HttpStatus.BAD_REQUEST, "Jwt_4001", "잘못된 JWT Token 타입입니다."),

    // 알 수 없는 에러
    _UNKNOWN_ERROR(HttpStatus.INTERNAL_SERVER_ERROR, "Server_5000", "알 수 없는 에러가 발생하였습니다."),
    _CONCURRENT_UPDATE_ERROR(HttpStatus.INTERNAL_SERVER_ERROR, "Server_5001", "동시 업데이트 에러가 발생하였습니다."),

    // S3
    _UPLOAD_FILE_ERROR(HttpStatus.INTERNAL_SERVER_ERROR, "S3_5001", "S3 이미지 업로드에 실패하였습니다."),
    _DELETE_FILE_ERROR(HttpStatus.INTERNAL_SERVER_ERROR, "S3_5002", "S3 이미지 제거에 실패하였습니다."),
    _FILE_NOT_FOUND(HttpStatus.NOT_FOUND, "S3_5003", "파일이 S3에 존재하지 않습니다."),

    // email
    _SEND_EMAIL_ERROR(HttpStatus.INTERNAL_SERVER_ERROR, "Email_5000", "이메일 전송에 실패하였습니다."),

    INVALID_EMAIL_AUTH_CODE(HttpStatus.BAD_REQUEST, "Email_4001", "인증 코드가 일치하지 않습니다."),
    EMAIL_AUTH_CODE_EXPIRED(HttpStatus.BAD_REQUEST, "Email_4002", "인증 코드가 만료되었습니다."),
    EMAIL_AUTH_CODE_SEND(HttpStatus.BAD_REQUEST, "Email_4003", "인증 코드 전송에 실패하였습니다."),
    EMAIL_AUTH_CODE_NOT_FOUND(HttpStatus.NOT_FOUND, "Email_4004", "인증 코드가 존재하지 않습니다."),

    // password
    _INVALID_PASSWORD(HttpStatus.BAD_REQUEST, "Password_4001", "비밀번호가 일치하지 않습니다."),
    _INVALID_PASSWORD_FORMAT(HttpStatus.BAD_REQUEST, "Password_4002", "비밀번호 형식이 올바르지 않습니다."),

    // temporary member
    TEMPORARY_MEMBER_NOT_FOUND(HttpStatus.NOT_FOUND, "TemporaryMember_4001", "임시 회원 정보가 존재하지 않습니다."),

    // member
    MEMBER_NOT_FOUND(HttpStatus.NOT_FOUND, "Member_4001", "회원 정보가 존재하지 않습니다."),
    MEMBER_NOT_EXIST_EMAIL(HttpStatus.NOT_FOUND, "Member_4002", "존재하지 않는 이메일입니다."),

    // Unauthorized Error
    INVALID_HEADER_ERROR(HttpStatus.UNAUTHORIZED, "Member_40108", "헤더가 올바르지 않습니다."),

    // Register member
    ALREADY_EXIST_EMAIL(HttpStatus.BAD_REQUEST, "Member_40200", "이미 존재하는 이메일입니다."),

    // Access Denied Error
    ACCESS_DENIED(HttpStatus.FORBIDDEN, "Access_40300", "접근 권한이 없습니다."),
    NOT_LOGIN_USER(HttpStatus.FORBIDDEN, "Access_40301", "로그인하지 않은 사용자입니다."),
    ARGUMENT_BAD_REQUEST(HttpStatus.BAD_REQUEST, "Argument_4001", "입력된 값이 잘못되었습니다."),


    // Friend
    FRIEND_NOT_FOUND(HttpStatus.NOT_FOUND, "Friend_4001", "친구 정보가 존재하지 않습니다."),
    FRIEND_PENDING_NOT_FOUND(HttpStatus.NOT_FOUND, "Friend_4002", "친구 요청한 기록이 없습니다."),
    UNAUTHORIZED_ACCESS(HttpStatus.FORBIDDEN, "Friend_4030", "현재 사용자는 해당 친구 관계에 대한 권한이 없습니다."),
    UNAUTHORIZED_ACTION(HttpStatus.FORBIDDEN, "Friend_4031", "본인의 친구 요청에 대해서는 해당 작업을 수행할 수 없습니다."),

    FRIEND_ALREADY_PENDING_EXISTS(HttpStatus.BAD_REQUEST, "Friend_4003", "이미 친구요청을 한 상태입니다."),
    FRIEND_IS_NOT_PENDING(HttpStatus.BAD_REQUEST, "Friend_4003", "친구 상태가 친구 요청 상태가 아닙니다."),
    FRIEND_ALREADY_ACCEPTED_EXISTS(HttpStatus.BAD_REQUEST, "Friend_4004", "이미 친구인 관계입니다."),
    FRIEND_ALREADY_REJECTED_EXISTS(HttpStatus.BAD_REQUEST, "Friend_4005", "이미 친구 신청을 거절당했습니다."),

    ;

    private final HttpStatus status;
    private final String errorCode;
    private final String message;

    FailType(final HttpStatus status, final String errorCode, final String message) {
        this.status = status;
        this.errorCode = errorCode;
        this.message = message;
    }
}
