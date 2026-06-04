package io.github.grantchen2003.cdb.tx.manager.storageengine;

import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;

@Component
public class RedisStorageEngine implements StorageEngine {

    private final RedisTemplate<String, String> redisTemplate;

    public RedisStorageEngine(RedisTemplate<String, String> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    @PostConstruct
    public void verifyConnection() {
        try {
            final String pong = redisTemplate.getConnectionFactory().getConnection().ping();
            System.out.println("Redis connection OK: " + pong);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to connect to Redis", e);
        }
    }

    @Override
    public ItemLookupResults getItems(List<ItemLookup> itemLookups) {
        final List<Object> raw = redisTemplate.execute(new SessionCallback<>() {
            @Override
            public List<Object> execute(RedisOperations ops) throws DataAccessException {
                ops.multi();
                ops.opsForHash().get("metadata", "seq_num");
                for (ItemLookup itemLookup : itemLookups) {
                    ops.opsForHash().get(itemLookup.table(), itemLookup.primaryKeyValue());
                }
                return ops.exec();
            }
        });

        final long seqNum = Long.parseLong((String) raw.getFirst());

        final List<String> data = new ArrayList<>();
        for (int i = 1; i < raw.size(); i++) {
            data.add((String) raw.get(i));
        }

        return new ItemLookupResults(seqNum, data);
    }

    @Override
    public long getSeqNum() {
        return Long.parseLong((String) redisTemplate.opsForHash().get("metadata", "seq_num"));
    }
}