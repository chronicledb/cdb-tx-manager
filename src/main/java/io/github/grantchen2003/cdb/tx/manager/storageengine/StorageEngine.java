package io.github.grantchen2003.cdb.tx.manager.storageengine;

import java.util.List;

public interface StorageEngine {
    record ItemLookup(String table, String primaryKeyValue) {}
    record ItemLookupResults(long seqNum, List<String> data) {}

    ItemLookupResults getItems(List<ItemLookup> itemLookups);
    long getSeqNum();
}
