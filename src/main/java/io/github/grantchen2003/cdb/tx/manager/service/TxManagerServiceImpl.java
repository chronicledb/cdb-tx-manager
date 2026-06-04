package io.github.grantchen2003.cdb.tx.manager.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.grantchen2003.cdb.tx.manager.chronicle.ChronicleServiceClient;
import io.github.grantchen2003.cdb.tx.manager.grpc.CommitTransactionRequest;
import io.github.grantchen2003.cdb.tx.manager.grpc.CommitTransactionResponse;
import io.github.grantchen2003.cdb.tx.manager.grpc.GetItemsRequest;
import io.github.grantchen2003.cdb.tx.manager.grpc.GetItemsResponse;
import io.github.grantchen2003.cdb.tx.manager.grpc.GetSeqNumResponse;
import io.github.grantchen2003.cdb.tx.manager.grpc.QueryResult;
import io.github.grantchen2003.cdb.tx.manager.grpc.TxManagerServiceGrpc;
import io.github.grantchen2003.cdb.tx.manager.storageengine.StorageEngine;
import io.github.grantchen2003.cdb.tx.manager.tx.Operation;
import io.github.grantchen2003.cdb.tx.manager.tx.Transaction;
import io.github.grantchen2003.cdb.tx.manager.writeschema.WriteSchema;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

@GrpcService
public class TxManagerServiceImpl extends TxManagerServiceGrpc.TxManagerServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(TxManagerServiceImpl.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final ChronicleServiceClient chronicleServiceClient;
    private final StorageEngine storageEngine;
    private final WriteSchema writeSchema;

    public TxManagerServiceImpl(
            ChronicleServiceClient chronicleServiceClient,
            StorageEngine storageEngine,
            WriteSchema writeSchema) {
        this.chronicleServiceClient = chronicleServiceClient;
        this.storageEngine = storageEngine;
        this.writeSchema = writeSchema;
    }

    @Override
    public void commitTransaction(CommitTransactionRequest request, StreamObserver<CommitTransactionResponse> responseObserver) {
        log.info("----------------------------------------------------");

        final long start = System.currentTimeMillis();

        log.info("[commitTransaction] PAYLOAD | seqNum={}", request.getSeqNum());

        final Transaction tx = new Transaction(
                request.getSeqNum(),
                request.getOperationsList().stream()
                        .map(op -> new Operation(
                                Operation.OpType.valueOf(op.getOpType().name()),
                                op.getTable(),
                                op.getData()
                        ))
                        .toList()
        );

        for (final Operation op : tx.operations()) {
            final String validationError = validateAgainstWriteSchema(op);
            if (validationError != null) {
                log.warn("[commitTransaction] VALIDATION_FAIL | seqNum={} error='{}'", request.getSeqNum(), validationError);
                responseObserver.onNext(CommitTransactionResponse.newBuilder()
                        .setStatus(CommitTransactionResponse.Code.FAILURE)
                        .setCommittedSeqNum(tx.seqNum())
                        .setErrorMessage(validationError)
                        .build());
                responseObserver.onCompleted();
                return;
            }
        }

        final long chronicleStart = System.currentTimeMillis();
        final ChronicleServiceClient.TxAppendResult appendTxResult = chronicleServiceClient.appendTx(tx);
        final long chronicleDuration = System.currentTimeMillis() - chronicleStart;

        final CommitTransactionResponse response;
        if (appendTxResult.success()) {
            log.info("[commitTransaction] OK  | seqNum={} committedSeqNum={} chronicleMs={} totalMs={}",
                    request.getSeqNum(), appendTxResult.committedSeqNum(), chronicleDuration, System.currentTimeMillis() - start);
            response = CommitTransactionResponse.newBuilder()
                    .setStatus(CommitTransactionResponse.Code.SUCCESS)
                    .setCommittedSeqNum(appendTxResult.committedSeqNum())
                    .build();
        } else {
            log.warn("[commitTransaction] FAIL | seqNum={} error='{}' chronicleMs={} totalMs={}",
                    request.getSeqNum(), appendTxResult.errorMessage(), chronicleDuration, System.currentTimeMillis() - start);
            response = CommitTransactionResponse.newBuilder()
                    .setStatus(CommitTransactionResponse.Code.FAILURE)
                    .setCommittedSeqNum(appendTxResult.committedSeqNum())
                    .setErrorMessage(appendTxResult.errorMessage())
                    .build();
        }

        responseObserver.onNext(response);
        responseObserver.onCompleted();

        log.info("----------------------------------------------------");
    }

    @Override
    public void getItems(GetItemsRequest request, StreamObserver<GetItemsResponse> responseObserver) {
        final List<StorageEngine.ItemLookup> itemLookups = request.getQueriesList().stream()
                .map(q -> new StorageEngine.ItemLookup(q.getTable(), q.getPrimaryKeyValue()))
                .toList();

        final StorageEngine.ItemLookupResults results = storageEngine.getItems(itemLookups);

        final GetItemsResponse.Builder responseBuilder = GetItemsResponse.newBuilder()
                .setSeqNum(results.seqNum());

        results.data().stream()
                .map(data -> data == null
                        ? QueryResult.newBuilder().setFound(false).build()
                        : QueryResult.newBuilder().setFound(true).setData(data).build())
                .forEach(responseBuilder::addResults);

        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void getSeqNum(com.google.protobuf.Empty request, StreamObserver<GetSeqNumResponse> responseObserver) {
        final GetSeqNumResponse response = GetSeqNumResponse.newBuilder()
                .setSeqNum(storageEngine.getSeqNum())
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private String validateAgainstWriteSchema(Operation op) {
        final String tableName = op.table();
        final Map<String, WriteSchema.TableDefinition> tables = writeSchema.tables();

        if (!tables.containsKey(tableName)) {
            return String.format("Unknown table '%s'. Allowed tables: %s", tableName, tables.keySet());
        }

        final WriteSchema.TableDefinition tableDef = tables.get(tableName);
        final Map<String, String> allowedAttributes = tableDef.attributeTypes();

        final Map<String, Object> data = parseData(op.data());
        if (data == null) {
            return String.format("Failed to parse data for operation on table '%s': not valid JSON", tableName);
        }

        for (final String field : data.keySet()) {
            if (!allowedAttributes.containsKey(field)) {
                return String.format(
                        "Unknown attribute '%s' on table '%s'. Allowed attributes: %s",
                        field, tableName, allowedAttributes.keySet()
                );
            }
        }

        return null;
    }

    private Map<String, Object> parseData(String dataJson) {
        try {
            return OBJECT_MAPPER.readValue(dataJson, new TypeReference<>() {});
        } catch (Exception e) {
            return null;
        }
    }
}
