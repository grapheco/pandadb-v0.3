package cn.pandadb.kernel.kv.db.raftdb

import java.util.List
import java.util.Queue
import cn.pandadb.kernel.kv.db.KeyValueIterator
import com.alipay.sofa.jraft.rhea.client.DefaultRheaKVStore
import com.alipay.sofa.jraft.rhea.client.pd.PlacementDriverClient
import com.alipay.sofa.jraft.rhea.storage.KVEntry
import com.alipay.sofa.jraft.util.BytesUtil

import scala.collection.mutable;

class RaftKVIterator(store: DefaultRheaKVStore) extends KeyValueIterator{

    private val pdClient: PlacementDriverClient = store.getPlacementDriverClient()
    private val endKey: Array[Byte] = null
    private val readOnlySafe: Boolean = false
    private val returnValue: Boolean = true
    private val bufSize: Int = 1024
    private val buf: Queue[KVEntry] = new java.util.LinkedList[KVEntry]()
    private var bufHere: KVEntry = null
    private var cursorKey: Array[Byte] = null

    override def next(): Unit = {
        bufHere = buf.remove()
    }

    override def isValid: Boolean = {
        if(buf.isEmpty()){
            while (this.endKey == null || BytesUtil.compare(this.cursorKey, this.endKey) < 0) {
                val kvEntries: List[KVEntry] = store.singleRegionScan(this.cursorKey, this.endKey,
                    this.bufSize, this.readOnlySafe, this.returnValue)
                if (kvEntries.isEmpty()) {
                    // cursorKey jump to next region's startKey
                    this.cursorKey = this.pdClient.findStartKeyOfNextRegion(this.cursorKey, false);
                    if (cursorKey == null) { // current is the last region
                       return false
                    }
                } else {
                    val last = kvEntries.get(kvEntries.size() - 1);
                    this.cursorKey = BytesUtil.nextBytes(last.getKey()); // cursorKey++
                    this.buf.addAll(kvEntries);
                    return true
                }
            }
            return false
        }
        true
    }

    override def seekToFirst(): Unit = {
        cursorKey = null
    }

    override def seekToLast(): Unit = ???

    override def seek(key: Array[Byte]): Unit = {
        cursorKey = key
    }

    override def key(): Array[Byte] = {
        bufHere.getKey
    }

    override def value(): Array[Byte] = {
        bufHere.getKey
    }

    override def prev(): Unit = ???

    override def seekForPrev(key: Array[Byte]): Unit = ???

}
