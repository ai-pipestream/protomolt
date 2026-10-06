package ai.protomolt.proto.repo.blob.redis;

import java.nio.charset.StandardCharsets;

/** Single-object atomic operations for standalone Redis 7+. Scripts do not provide rollback on server errors. */
final class RedisObjectScripts {
    private RedisObjectScripts() {}
    private static byte[] script(String value) { return value.getBytes(StandardCharsets.UTF_8); }
    private static final String CHECK = """
            if redis.call('EXISTS',KEYS[1])==0 then return {0} end
            if redis.call('TYPE',KEYS[1]).ok~='hash' then return {3} end
            for _,field in ipairs({'data','content_type','etag','last_modified_ms','attributes'}) do
              if redis.call('HEXISTS',KEYS[1],field)==0 then return {3} end
            end
            """;
    private static final String WRITE = """
            local attributes={}
            for i=6,#ARGV,2 do attributes[ARGV[i]]=ARGV[i+1] end
            local packed=cmsgpack.pack(attributes)
            redis.call('HSET',KEYS[1],'data',ARGV[1],'content_type',ARGV[2],'etag',ARGV[3],
              'last_modified_ms',ARGV[4],'attributes',packed)
            if ARGV[5]=='0' then redis.call('PERSIST',KEYS[1]) else redis.call('EXPIRE',KEYS[1],ARGV[5]) end
            return 1
            """;
    static final byte[] PUT = script(WRITE);
    // The condition is the final argument; remove it before packing metadata pairs.
    static final byte[] CONDITIONAL_PUT = script("""
            local expected=table.remove(ARGV)
            local exists=redis.call('EXISTS',KEYS[1])~=0
            if exists then
              if redis.call('TYPE',KEYS[1]).ok~='hash' then return -1 end
              for _,field in ipairs({'data','content_type','etag','last_modified_ms','attributes'}) do
                if redis.call('HEXISTS',KEYS[1],field)==0 then return -1 end
              end
              local tag=redis.call('HGET',KEYS[1],'etag')
              if #tag<3 or #tag>1024 or string.sub(tag,1,1)~='"' or string.sub(tag,-1)~='"' then return -1 end
              for i=2,#tag-1 do
                local b=string.byte(tag,i)
                if b~=33 and (b<35 or b>126) then return -1 end
              end
              if expected=='' or tag~=expected then return 0 end
            elseif expected~='' then return 0 end
            """ + WRITE);
    static final byte[] GET = script(CHECK + """
            local size=redis.call('HSTRLEN',KEYS[1],'data')
            if size>tonumber(ARGV[1]) then return {2} end
            return {1,redis.call('HGET',KEYS[1],'data'),redis.call('HGET',KEYS[1],'content_type'),redis.call('HGET',KEYS[1],'etag')}
            """);
    static final byte[] STAT = script(CHECK + """
            return {1,redis.call('HSTRLEN',KEYS[1],'data'),redis.call('HGET',KEYS[1],'last_modified_ms')}
            """);
    static final byte[] COPY = script(CHECK + """
            if redis.call('HSTRLEN',KEYS[1],'data')>tonumber(ARGV[1]) then return {2} end
            if KEYS[1]~=KEYS[2] then redis.call('COPY',KEYS[1],KEYS[2],'REPLACE') end
            redis.call('HSET',KEYS[2],'last_modified_ms',ARGV[2])
            if ARGV[3]=='0' then redis.call('PERSIST',KEYS[2]) else redis.call('EXPIRE',KEYS[2],ARGV[3]) end
            return {1}
            """);
    static final byte[] RECLAIM = script("redis.call('DEL',KEYS[1]); return redis.call('EXISTS',KEYS[1])");
}
