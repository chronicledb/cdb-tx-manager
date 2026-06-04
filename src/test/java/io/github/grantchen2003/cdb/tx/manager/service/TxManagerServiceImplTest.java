package io.github.grantchen2003.cdb.tx.manager.service;

import io.github.grantchen2003.cdb.tx.manager.chronicle.ChronicleServiceClient;
import io.github.grantchen2003.cdb.tx.manager.grpc.CommitTransactionRequest;
import io.github.grantchen2003.cdb.tx.manager.grpc.CommitTransactionResponse;
import io.github.grantchen2003.cdb.tx.manager.grpc.GetItemsRequest;
import io.github.grantchen2003.cdb.tx.manager.grpc.GetItemsResponse;
import io.github.grantchen2003.cdb.tx.manager.grpc.GetSeqNumResponse;
import io.github.grantchen2003.cdb.tx.manager.grpc.Operation;
import io.github.grantchen2003.cdb.tx.manager.grpc.Query;
import io.github.grantchen2003.cdb.tx.manager.storageengine.StorageEngine;
import io.github.grantchen2003.cdb.tx.manager.tx.Transaction;
import io.github.grantchen2003.cdb.tx.manager.writeschema.WriteSchema;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TxManagerServiceImplTest {

    private ChronicleServiceClient chronicleServiceClientMock;
    private StorageEngine storageEngineMock;
    private WriteSchema writeSchemaMock;
    private StreamObserver<CommitTransactionResponse> commitObserverMock;
    private StreamObserver<GetItemsResponse> getItemsObserverMock;
    private TxManagerServiceImpl service;

    private static final String TABLE = "products";

    @BeforeEach
    void setUp() {
        chronicleServiceClientMock = mock(ChronicleServiceClient.class);
        storageEngineMock = mock(StorageEngine.class);
        writeSchemaMock = mock(WriteSchema.class);
        commitObserverMock = mock(StreamObserver.class);
        getItemsObserverMock = mock(StreamObserver.class);

        WriteSchema.TableDefinition tableDef = mock(WriteSchema.TableDefinition.class);
        when(tableDef.attributeTypes()).thenReturn(Map.of("eye", "string"));
        when(writeSchemaMock.tables()).thenReturn(Map.of(TABLE, tableDef));

        service = new TxManagerServiceImpl(chronicleServiceClientMock, storageEngineMock, writeSchemaMock);
    }

    // -------------------------------------------------------------------------
    // helpers
    // -------------------------------------------------------------------------

    private CommitTransactionRequest buildCommitRequest(long seqNum, String opType, String table, String data) {
        return CommitTransactionRequest.newBuilder()
                .setSeqNum(seqNum)
                .addOperations(Operation.newBuilder()
                        .setOpType(Operation.OpType.valueOf(opType))
                        .setTable(table)
                        .setData(data)
                        .build())
                .build();
    }

    private GetItemsRequest buildGetItemsRequest(String[]... tableKeyPairs) {
        GetItemsRequest.Builder builder = GetItemsRequest.newBuilder();
        for (String[] pair : tableKeyPairs) {
            builder.addQueries(Query.newBuilder()
                    .setTable(pair[0])
                    .setPrimaryKeyValue(pair[1])
                    .build());
        }
        return builder.build();
    }

    private StorageEngine.ItemLookupResults mockLookupResults(long seqNum, List<String> data) {
        StorageEngine.ItemLookupResults results = mock(StorageEngine.ItemLookupResults.class);
        when(results.seqNum()).thenReturn(seqNum);
        when(results.data()).thenReturn(data);
        return results;
    }

    // -------------------------------------------------------------------------
    // commitTransaction
    // -------------------------------------------------------------------------

    @Test
    void commitTransaction_validRequest_returnsSuccess() {
        when(chronicleServiceClientMock.appendTx(any(Transaction.class)))
                .thenReturn(new ChronicleServiceClient.TxAppendResult(true, 2L, ""));

        service.commitTransaction(buildCommitRequest(1L, "PUT", TABLE, "{\"eye\":\"some-value\"}"), commitObserverMock);

        verify(commitObserverMock).onNext(argThat(r ->
                r.getStatus() == CommitTransactionResponse.Code.SUCCESS &&
                        r.getCommittedSeqNum() == 2L &&
                        r.getErrorMessage().isEmpty()
        ));
        verify(commitObserverMock).onCompleted();
    }

    @Test
    void commitTransaction_unknownTable_returnsFailureWithError() {
        service.commitTransaction(buildCommitRequest(1L, "PUT", "unknown_table", "{\"eye\":\"val\"}"), commitObserverMock);

        verify(chronicleServiceClientMock, never()).appendTx(any());
        verify(commitObserverMock).onNext(argThat(r ->
                r.getStatus() == CommitTransactionResponse.Code.FAILURE &&
                        r.getErrorMessage().contains("Unknown table")
        ));
        verify(commitObserverMock).onCompleted();
    }

    @Test
    void commitTransaction_invalidJson_returnsFailureWithError() {
        service.commitTransaction(buildCommitRequest(1L, "PUT", TABLE, "not-json"), commitObserverMock);

        verify(chronicleServiceClientMock, never()).appendTx(any());
        verify(commitObserverMock).onNext(argThat(r ->
                r.getStatus() == CommitTransactionResponse.Code.FAILURE &&
                        !r.getErrorMessage().isEmpty()
        ));
        verify(commitObserverMock).onCompleted();
    }

    @Test
    void commitTransaction_chronicleReturnsFailure_passesThrough() {
        when(chronicleServiceClientMock.appendTx(any(Transaction.class)))
                .thenReturn(new ChronicleServiceClient.TxAppendResult(false, 2L, ""));

        service.commitTransaction(buildCommitRequest(1L, "PUT", TABLE, "{\"eye\":\"some-value\"}"), commitObserverMock);

        verify(commitObserverMock).onNext(argThat(r ->
                r.getStatus() == CommitTransactionResponse.Code.FAILURE &&
                        r.getCommittedSeqNum() == 2L
        ));
        verify(commitObserverMock).onCompleted();
    }

    // -------------------------------------------------------------------------
    // getItems
    // -------------------------------------------------------------------------

    @Test
    void getItems_emptyQueryList_returnsEmptyResults() {
        StorageEngine.ItemLookupResults results = mockLookupResults(5L, List.of());
        when(storageEngineMock.getItems(anyList())).thenReturn(results);

        service.getItems(buildGetItemsRequest(), getItemsObserverMock);

        verify(getItemsObserverMock).onNext(argThat(r ->
                r.getSeqNum() == 5L &&
                        r.getResultsList().isEmpty()
        ));
        verify(getItemsObserverMock).onCompleted();
    }

    @Test
    void getItems_singleItemFound_returnsFoundWithData() {
        StorageEngine.ItemLookupResults results = mockLookupResults(10L, List.of("{\"eye\":\"blue\"}"));
        when(storageEngineMock.getItems(anyList())).thenReturn(results);

        service.getItems(buildGetItemsRequest(new String[]{TABLE, "pk-1"}), getItemsObserverMock);

        verify(getItemsObserverMock).onNext(argThat(r ->
                r.getSeqNum() == 10L &&
                        r.getResultsCount() == 1 &&
                        r.getResults(0).getFound() &&
                        r.getResults(0).getData().equals("{\"eye\":\"blue\"}")
        ));
        verify(getItemsObserverMock).onCompleted();
    }

    @Test
    void getItems_singleItemNotFound_returnsNotFound() {
        StorageEngine.ItemLookupResults results = mockLookupResults(10L, Arrays.asList((String) null));
        when(storageEngineMock.getItems(anyList())).thenReturn(results);

        service.getItems(buildGetItemsRequest(new String[]{TABLE, "pk-missing"}), getItemsObserverMock);

        verify(getItemsObserverMock).onNext(argThat(r ->
                r.getResultsCount() == 1 &&
                        !r.getResults(0).getFound() &&
                        r.getResults(0).getData().isEmpty()
        ));
        verify(getItemsObserverMock).onCompleted();
    }

    @Test
    void getItems_mixedResults_preservesOrderAndFoundness() {
        StorageEngine.ItemLookupResults results = mockLookupResults(20L, Arrays.asList("{\"eye\":\"green\"}", null, "{\"eye\":\"brown\"}"));
        when(storageEngineMock.getItems(anyList())).thenReturn(results);

        service.getItems(buildGetItemsRequest(
                new String[]{TABLE, "pk-1"},
                new String[]{TABLE, "pk-missing"},
                new String[]{TABLE, "pk-3"}
        ), getItemsObserverMock);

        verify(getItemsObserverMock).onNext(argThat(r ->
                r.getSeqNum() == 20L &&
                        r.getResultsCount() == 3 &&
                        r.getResults(0).getFound()  && r.getResults(0).getData().equals("{\"eye\":\"green\"}") &&
                        !r.getResults(1).getFound() && r.getResults(1).getData().isEmpty() &&
                        r.getResults(2).getFound()  && r.getResults(2).getData().equals("{\"eye\":\"brown\"}")
        ));
        verify(getItemsObserverMock).onCompleted();
    }

    @Test
    void getItems_seqNumPassedThrough() {
        StorageEngine.ItemLookupResults results = mockLookupResults(42L, List.of("{\"eye\":\"hazel\"}"));
        when(storageEngineMock.getItems(anyList())).thenReturn(results);

        service.getItems(buildGetItemsRequest(new String[]{TABLE, "pk-1"}), getItemsObserverMock);

        verify(getItemsObserverMock).onNext(argThat(r -> r.getSeqNum() == 42L));
        verify(getItemsObserverMock).onCompleted();
    }

    // -------------------------------------------------------------------------
    // getSeqNum
    // -------------------------------------------------------------------------

    @Test
    void getSeqNum_returnsSeqNumFromStorageEngine() {
        StreamObserver<GetSeqNumResponse> getSeqNumObserverMock = mock(StreamObserver.class);
        when(storageEngineMock.getSeqNum()).thenReturn(42L);

        service.getSeqNum(com.google.protobuf.Empty.getDefaultInstance(), getSeqNumObserverMock);

        verify(getSeqNumObserverMock).onNext(argThat(r -> r.getSeqNum() == 42L));
        verify(getSeqNumObserverMock).onCompleted();
    }

    @Test
    void getSeqNum_returnsZeroWhenSeqNumIsZero() {
        StreamObserver<GetSeqNumResponse> getSeqNumObserverMock = mock(StreamObserver.class);
        when(storageEngineMock.getSeqNum()).thenReturn(0L);

        service.getSeqNum(com.google.protobuf.Empty.getDefaultInstance(), getSeqNumObserverMock);

        verify(getSeqNumObserverMock).onNext(argThat(r -> r.getSeqNum() == 0L));
        verify(getSeqNumObserverMock).onCompleted();
    }

    @Test
    void getSeqNum_returnsLargeSeqNum() {
        StreamObserver<GetSeqNumResponse> getSeqNumObserverMock = mock(StreamObserver.class);
        when(storageEngineMock.getSeqNum()).thenReturn(Long.MAX_VALUE);

        service.getSeqNum(com.google.protobuf.Empty.getDefaultInstance(), getSeqNumObserverMock);

        verify(getSeqNumObserverMock).onNext(argThat(r -> r.getSeqNum() == Long.MAX_VALUE));
        verify(getSeqNumObserverMock).onCompleted();
    }

    @Test
    void getSeqNum_onlyCallsOnNextOnce() {
        StreamObserver<GetSeqNumResponse> getSeqNumObserverMock = mock(StreamObserver.class);
        when(storageEngineMock.getSeqNum()).thenReturn(7L);

        service.getSeqNum(com.google.protobuf.Empty.getDefaultInstance(), getSeqNumObserverMock);

        verify(getSeqNumObserverMock, times(1)).onNext(any());
        verify(getSeqNumObserverMock, times(1)).onCompleted();
        verify(getSeqNumObserverMock, never()).onError(any());
    }
}