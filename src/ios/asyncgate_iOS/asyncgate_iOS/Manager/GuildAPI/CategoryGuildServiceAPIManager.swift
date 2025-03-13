//
//  CategoryGuildServiceAPIManager.swift
//  asyncgate_iOS
//
//  Created by kdk on 2/19/25.
//

import Alamofire

// MARK: Manager - Guild Category Service API 매니저
class CategoryGuildServiceAPIManager {
    static let shared = CategoryGuildServiceAPIManager()
    
    // 호출 - 엑세스 토큰 사용 및 API 주소
    private let accessTokenViewModel = AccessTokenViewModel.shared
    private let hostUrl = Config.shared.hostUrl
    
    // MARK: 함수 - 카테고리 생성
    func createGuildCategory(name: String, guildId: String, isPrivate: Bool, completion: @escaping (Result<CategoryResponse, ErrorResponse>) -> Void) {
        let url = "https://\(hostUrl)/guilds/category"
        
        let parameters: [String: Any] = [
            "name": name,
            "guildId": guildId,
            "private": isPrivate
        ]
        
        if let accessToken = accessTokenViewModel.accessToken {
            let headers: HTTPHeaders = [
                "Authorization": "Bearer \(accessToken)",
            ]
            
            AF.request(url, method: .post, parameters: parameters, encoding: JSONEncoding.default, headers: headers)
                .validate()
                .responseDecodable(of: CategoryResponse.self) { response in
                    switch response.result {
                    case .success(let successResponse):
                        completion(.success(successResponse))
                      
                    case .failure(_):
                        if let data = response.data {
                            do {
                                let errorResponse = try JSONDecoder().decode(ErrorResponse.self, from: data)
                                completion(.failure(errorResponse))
                               
                            } catch {
                                completion(.failure(ErrorResponse(timeStamp: "", path: "", status: 0, error: "오류가 발생했습니다.", requestId: "")))
                            }
                        } else {
                            completion(.failure(ErrorResponse(timeStamp: "", path: "", status: 1, error: "서버와 연결할 수 없습니다. 다시 시도해주세요.", requestId: "")))
                        }
                    }
                }
        }
    }
    
    // MARK: 함수 - 카테고리 삭제
    func deleteGuildCategory(guildId: String, categoryId: String, completion: @escaping (Result<SuccessResultStringResponse, ErrorResponse>) -> Void) {
        let url = "https://\(hostUrl)/guilds/category/\(guildId)/\(categoryId)"
        
        if let accessToken = accessTokenViewModel.accessToken {
            let headers: HTTPHeaders = [
                "Authorization": "Bearer \(accessToken)",
            ]
            
            AF.request(url, method: .delete, encoding: JSONEncoding.default, headers: headers)
                .validate()
                .responseDecodable(of: SuccessResultStringResponse.self) { response in
                    switch response.result {
                    case .success(let successResponse):
                        completion(.success(successResponse))
                        
                    case .failure(_):
                        if let data = response.data {
                            do {
                                let errorResponse = try JSONDecoder().decode(ErrorResponse.self, from: data)
                                completion(.failure(errorResponse))
                               
                            } catch {
                                completion(.failure(ErrorResponse(timeStamp: "", path: "", status: 0, error: "오류가 발생했습니다.", requestId: "")))
                            }
                        } else {
                            completion(.failure(ErrorResponse(timeStamp: "", path: "", status: 1, error: "서버와 연결할 수 없습니다. 다시 시도해주세요.", requestId: "")))
                        }
                    }
                }
        }
    }
}
