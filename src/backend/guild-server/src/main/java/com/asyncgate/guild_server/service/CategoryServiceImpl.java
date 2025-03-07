package com.asyncgate.guild_server.service;

import com.asyncgate.guild_server.domain.Category;
import com.asyncgate.guild_server.domain.GuildMember;
import com.asyncgate.guild_server.domain.GuildRole;
import com.asyncgate.guild_server.dto.request.CategoryRequest;
import com.asyncgate.guild_server.dto.response.CategoryResponse;
import com.asyncgate.guild_server.repository.CategoryRepository;
import com.asyncgate.guild_server.repository.ChannelRepository;
import com.asyncgate.guild_server.repository.GuildMemberRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class CategoryServiceImpl implements CategoryService {

    private final CategoryRepository categoryRepository;
    private final GuildMemberRepository guildMemberRepository;
    private final ChannelRepository channelRepository;

    @Override
    @Transactional
    public CategoryResponse create(final String userId, final CategoryRequest request) {
        validatePermission(userId, request.getGuildId());
        Category category = Category.create(request.getName(), request.getGuildId(), request.isPrivate());
        categoryRepository.save(category);
        return CategoryResponse.from(category);
    }

    @Override
    @Transactional
    public void delete(final String userId, final String guildId, final String categoryId) {
        validatePermission(userId, guildId);
        categoryRepository.deleteById(categoryId);
        channelRepository.deleteAllByCategoryId(categoryId);
    }

    private void validatePermission(final String userId, final String guildId) {
        GuildMember guildMember = guildMemberRepository.findAcceptedMemberByUserIdAndGuildId(userId, guildId);
        // ToDo guildMember에 저장되어있는 카테고리 생성할 수 있는지 권한 확인

        if (!guildMember.getGuildRole().equals(GuildRole.ADMIN)) {
        }
    }
}
