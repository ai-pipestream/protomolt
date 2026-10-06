package ai.protomolt.proto.repo.blob.redis;

/** Explicit physical write policy; create-only keys use a separate layout. */
public enum RedisWritePolicy { REPLACE, CREATE_ONLY }
