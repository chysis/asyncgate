package com.asyncgate.guild_server.support.utility;

import com.asyncgate.guild_server.domain.*;
import com.asyncgate.guild_server.entity.*;

public class DomainUtil {

    public static class GuildMapper {
        public static GuildEntity toEntity(final Guild guild) {
            return GuildEntity.builder()
                    .id(guild.getId())
                    .name(guild.getName())
                    .isPrivate(guild.isPrivate())
                    .profileImageUrl(guild.getProfileImageUrl())
                    .build();
        }

        public static Guild toDomain(final GuildEntity entity) {
            return Guild.builder()
                    .id(entity.getId())
                    .name(entity.getName())
                    .isPrivate(entity.isPrivate())
                    .profileImageUrl(entity.getProfileImageUrl())
                    .build();
        }
    }

    public static class GuildMemberMapper {
        public static GuildMemberEntity toEntity(final GuildMember guildMember) {
            return GuildMemberEntity.builder()
                    .id(guildMember.getId())
                    .userId(guildMember.getUserId())
                    .guildId(guildMember.getGuildId())
                    .guildRole(guildMember.getGuildRole())
                    .status(guildMember.getStatus())
                    .build();
        }

        public static GuildMember toDomain(final GuildMemberEntity entity) {
            return GuildMember.builder()
                    .id(entity.getId())
                    .userId(entity.getUserId())
                    .guildId(entity.getGuildId())
                    .guildRole(entity.getGuildRole())
                    .status(entity.getStatus())
                    .build();
        }
    }

    public static class CategoryMapper {
        public static CategoryEntity toEntity(final Category category) {
            return CategoryEntity.builder()
                    .id(category.getId())
                    .name(category.getName())
                    .isPrivate(category.isPrivate())
                    .guildId(category.getGuildId())
                    .build();
        }

        public static Category toDomain(final CategoryEntity entity) {
            return Category.builder()
                    .id(entity.getId())
                    .name(entity.getName())
                    .isPrivate(entity.isPrivate())
                    .guildId(entity.getGuildId())
                    .build();
        }
    }

    public static class ChannelMapper {
        public static ChannelEntity toEntity(final Channel channel) {
            return ChannelEntity.builder()
                    .id(channel.getId())
                    .guildId(channel.getGuildId())
                    .categoryId(channel.getCategoryId())
                    .name(channel.getName())
                    .channelType(channel.getChannelType())
                    .isPrivate(channel.isPrivate())
                    .build();
        }

        public static Channel toDomain(final ChannelEntity entity) {
            return Channel.builder()
                    .id(entity.getId())
                    .guildId(entity.getGuildId())
                    .categoryId(entity.getCategoryId())
                    .name(entity.getName())
                    .channelType(entity.getChannelType())
                    .isPrivate(entity.isPrivate())
                    .build();
        }
    }

    public static class DirectMapper {
        public static DirectEntity toEntity(final Direct domain) {
            return DirectEntity.builder()
                    .id(domain.getId())
                    .build();
        }

        public static Direct toDomain(final DirectEntity entity) {
            return Direct.builder()
                    .id(entity.getId())
                    .createdDate(entity.getCreatedDate())
                    .build();
        }
    }

    public static class DirectMemberMapper {
        public static DirectMemberEntity toEntity(final DirectMember directMember) {
            return DirectMemberEntity.builder()
                    .id(directMember.getId())
                    .directId(directMember.getDirectId())
                    .memberId(directMember.getMemberId())
                    .memberName(directMember.getMemberName())
                    .build();
        }

        public static DirectMember toDomain(final DirectMemberEntity entity) {
            return DirectMember.builder()
                    .id(entity.getId())
                    .directId(entity.getDirectId())
                    .memberId(entity.getMemberId())
                    .memberName(entity.getMemberName())
                    .createdDate(entity.getCreatedDate())
                    .build();
        }
    }
}
