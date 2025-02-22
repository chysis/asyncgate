// result가 없는 기본 응답
export interface UserDefaultResponse {
  httpStatus: number;
  message: string;
  time: Date;
  result: null;
}

export interface PostLoginRequest {
  email: string;
  password: string;
}

export interface PostLoginResponse {
  httpStatus: number;
  message: string;
  time: Date;
  result: {
    access_token: string;
  };
}

export interface PostRegisterRequest {
  email: string;
  password: string;
  name: string;
  nickname: string;
  birth: string;
}

export type PostRegisterResponse = UserDefaultResponse;

export interface PostAuthCodeRequest {
  email: string;
  authentication_code: string;
}

export type PostAuthCodeResponse = UserDefaultResponse;

export interface PostEmailDuplicateResponse {
  httpStatus: number;
  message: string;
  time: Date;
  result: {
    is_duplicate: boolean;
  };
}

export interface PatchUserInfoRequest {
  name: string;
  nickname: string;
  profile_image: string;
}

export type PatchUserInfoResponse = UserDefaultResponse;

export type DeleteUserInfoResponse = UserDefaultResponse;
