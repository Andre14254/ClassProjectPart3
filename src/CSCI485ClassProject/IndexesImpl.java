package CSCI485ClassProject;

import CSCI485ClassProject.models.IndexType;
import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.PathUtil;
import com.apple.foundationdb.tuple.Tuple;
import CSCI485ClassProject.fdb.FDBHelper;
import CSCI485ClassProject.fdb.FDBKVPair;
import CSCI485ClassProject.models.Record;

import java.util.ArrayList;
import java.util.List;

public class IndexesImpl implements Indexes{
  private final Database db;
  public IndexesImpl() {
    db = FDBHelper.initialization();
  }
  @Override
  public StatusCode createIndex(String tableName, String attrName, IndexType indexType) {
    // your code
    Transaction tx = FDBHelper.openTransaction(db);
    //if (!FDBHelper.doesSubdirectoryExists(tx, tableName)) {
   //   FDBHelper.abortTransaction(tx);
    //  return StatusCode.TABLE_NOT_FOUND;
   // }
    List<String> table = new ArrayList<>();
    table.add(tableName);
    table.add(attrName);
    table.add("i");
    if (FDBHelper.doesSubdirectoryExists(tx, table)) {
      FDBHelper.abortTransaction(tx);
      return StatusCode.INDEX_ALREADY_EXISTS_ON_ATTRIBUTE;
    }
    DirectorySubspace dir = FDBHelper.createOrOpenSubspace(tx, table);
    RecordsImpl records = new RecordsImpl();
    Cursor cursor = records.openCursor(tableName, Cursor.Mode.READ);
    Record rec = records.getFirst(cursor);
    while (true) {
      rec = records.getNext(cursor);
      if (rec == null) {
        break;
      }
      Tuple kt = new Tuple();
      kt.add(rec.getValueForGivenAttrName(attrName).hashCode());
      FDBHelper.setFDBKVPair(dir, tx, new FDBKVPair(table, kt, new Tuple()));
    }
    return StatusCode.SUCCESS;
  }

  @Override
  public StatusCode dropIndex(String tableName, String attrName) {
    // your code
    return StatusCode.SUCCESS;
  }
}
