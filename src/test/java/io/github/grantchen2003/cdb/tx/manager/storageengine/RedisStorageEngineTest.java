package io.github.grantchen2003.cdb.tx.manager.storageengine;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RedisStorageEngineTest {

    private RedisTemplate<String, String> redisTemplateMock;
    private HashOperations<String, Object, Object> hashOpsMock;
    private RedisStorageEngine storageEngine;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        redisTemplateMock = mock(RedisTemplate.class);
        hashOpsMock = mock(HashOperations.class);
        when(redisTemplateMock.opsForHash()).thenReturn(hashOpsMock);
        storageEngine = new RedisStorageEngine(redisTemplateMock);
    }

    // -------------------------------------------------------------------------
    // verifyConnection
    // -------------------------------------------------------------------------

    @Test
    void verifyConnection_successfulPing_doesNotThrow() {
        RedisConnectionFactory factoryMock = mock(RedisConnectionFactory.class);
        RedisConnection connectionMock = mock(RedisConnection.class);
        when(redisTemplateMock.getConnectionFactory()).thenReturn(factoryMock);
        when(factoryMock.getConnection()).thenReturn(connectionMock);
        when(connectionMock.ping()).thenReturn("PONG");

        storageEngine.verifyConnection();

        verify(connectionMock).ping();
    }

    @Test
    void verifyConnection_pingThrows_wrapsInIllegalStateException() {
        RedisConnectionFactory factoryMock = mock(RedisConnectionFactory.class);
        when(redisTemplateMock.getConnectionFactory()).thenReturn(factoryMock);
        when(factoryMock.getConnection()).thenThrow(new RuntimeException("connection refused"));

        assertThatThrownBy(() -> storageEngine.verifyConnection())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Failed to connect to Redis")
                .hasCauseInstanceOf(RuntimeException.class);
    }

    @Test
    void verifyConnection_nullConnectionFactory_wrapsInIllegalStateException() {
        when(redisTemplateMock.getConnectionFactory()).thenReturn(null);

        assertThatThrownBy(() -> storageEngine.verifyConnection())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Failed to connect to Redis");
    }

    // -------------------------------------------------------------------------
    // getItems
    // -------------------------------------------------------------------------

    @Test
    @SuppressWarnings("unchecked")
    void getItems_singleLookup_returnsCorrectSeqNumAndData() {
        List<Object> redisResult = List.of("10", "{\"eye\":\"blue\"}");
        when(redisTemplateMock.execute(any(SessionCallback.class))).thenReturn(redisResult);

        List<StorageEngine.ItemLookup> lookups = List.of(
                new StorageEngine.ItemLookup("products", "pk-1")
        );

        StorageEngine.ItemLookupResults result = storageEngine.getItems(lookups);

        assertThat(result.seqNum()).isEqualTo(10L);
        assertThat(result.data()).containsExactly("{\"eye\":\"blue\"}");
    }

    @Test
    @SuppressWarnings("unchecked")
    void getItems_multipleLookups_returnsAllDataInOrder() {
        List<Object> redisResult = List.of("20", "{\"eye\":\"green\"}", "{\"eye\":\"brown\"}");
        when(redisTemplateMock.execute(any(SessionCallback.class))).thenReturn(redisResult);

        List<StorageEngine.ItemLookup> lookups = List.of(
                new StorageEngine.ItemLookup("products", "pk-1"),
                new StorageEngine.ItemLookup("products", "pk-2")
        );

        StorageEngine.ItemLookupResults result = storageEngine.getItems(lookups);

        assertThat(result.seqNum()).isEqualTo(20L);
        assertThat(result.data()).containsExactly("{\"eye\":\"green\"}", "{\"eye\":\"brown\"}");
    }

    @Test
    @SuppressWarnings("unchecked")
    void getItems_missingItem_propagatesNullInDataList() {
        List<Object> redisResult = Arrays.asList("15", null);
        when(redisTemplateMock.execute(any(SessionCallback.class))).thenReturn(redisResult);

        List<StorageEngine.ItemLookup> lookups = List.of(
                new StorageEngine.ItemLookup("products", "pk-missing")
        );

        StorageEngine.ItemLookupResults result = storageEngine.getItems(lookups);

        assertThat(result.seqNum()).isEqualTo(15L);
        assertThat(result.data()).hasSize(1);
        assertThat(result.data().get(0)).isNull();
    }

    @Test
    @SuppressWarnings("unchecked")
    void getItems_mixedFoundAndMissing_preservesNullsAndOrder() {
        List<Object> redisResult = Arrays.asList("30", "{\"eye\":\"hazel\"}", null, "{\"eye\":\"grey\"}");
        when(redisTemplateMock.execute(any(SessionCallback.class))).thenReturn(redisResult);

        List<StorageEngine.ItemLookup> lookups = List.of(
                new StorageEngine.ItemLookup("products", "pk-1"),
                new StorageEngine.ItemLookup("products", "pk-missing"),
                new StorageEngine.ItemLookup("products", "pk-3")
        );

        StorageEngine.ItemLookupResults result = storageEngine.getItems(lookups);

        assertThat(result.seqNum()).isEqualTo(30L);
        assertThat(result.data()).containsExactly("{\"eye\":\"hazel\"}", null, "{\"eye\":\"grey\"}");
    }

    @Test
    @SuppressWarnings("unchecked")
    void getItems_emptyLookupList_returnsEmptyDataWithSeqNum() {
        List<Object> redisResult = List.of("5");
        when(redisTemplateMock.execute(any(SessionCallback.class))).thenReturn(redisResult);

        StorageEngine.ItemLookupResults result = storageEngine.getItems(List.of());

        assertThat(result.seqNum()).isEqualTo(5L);
        assertThat(result.data()).isEmpty();
    }

    @Test
    @SuppressWarnings("unchecked")
    void getItems_executesTransactionWithCorrectKeys() {
        List<Object> redisResult = List.of("7", "{\"eye\":\"blue\"}");
        when(redisTemplateMock.execute(any(SessionCallback.class))).thenReturn(redisResult);

        // Capture the SessionCallback and execute it against mock RedisOperations
        // to verify multi/exec wrapping and correct hash key lookups
        ArgumentCaptor<SessionCallback<List<Object>>> callbackCaptor =
                ArgumentCaptor.forClass(SessionCallback.class);

        storageEngine.getItems(List.of(new StorageEngine.ItemLookup("products", "pk-1")));

        verify(redisTemplateMock).execute(callbackCaptor.capture());

        RedisOperations<String, String> opsMock = mock(RedisOperations.class);
        HashOperations<String, Object, Object> txHashOpsMock = mock(HashOperations.class);
        when(opsMock.opsForHash()).thenReturn(txHashOpsMock);
        when(opsMock.exec()).thenReturn(redisResult);

        callbackCaptor.getValue().execute(opsMock);

        verify(opsMock).multi();
        verify(txHashOpsMock).get("metadata", "seq_num");
        verify(txHashOpsMock).get("products", "pk-1");
        verify(opsMock).exec();
    }

    // -------------------------------------------------------------------------
    // getSeqNum
    // -------------------------------------------------------------------------

    @Test
    void getSeqNum_returnsSeqNumFromMetadataHash() {
        when(hashOpsMock.get("metadata", "seq_num")).thenReturn("42");

        assertThat(storageEngine.getSeqNum()).isEqualTo(42L);
    }

    @Test
    void getSeqNum_returnsZero() {
        when(hashOpsMock.get("metadata", "seq_num")).thenReturn("0");

        assertThat(storageEngine.getSeqNum()).isEqualTo(0L);
    }

    @Test
    void getSeqNum_returnsLargeValue() {
        when(hashOpsMock.get("metadata", "seq_num")).thenReturn(String.valueOf(Long.MAX_VALUE));

        assertThat(storageEngine.getSeqNum()).isEqualTo(Long.MAX_VALUE);
    }

    @Test
    void getSeqNum_lookupsCorrectHashKeyAndField() {
        when(hashOpsMock.get("metadata", "seq_num")).thenReturn("99");

        storageEngine.getSeqNum();

        verify(hashOpsMock).get(eq("metadata"), eq("seq_num"));
    }

    @Test
    void getSeqNum_nonNumericValue_throwsNumberFormatException() {
        when(hashOpsMock.get("metadata", "seq_num")).thenReturn("not-a-number");

        assertThatThrownBy(() -> storageEngine.getSeqNum())
                .isInstanceOf(NumberFormatException.class);
    }
}