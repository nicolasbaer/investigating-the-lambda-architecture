package ch.uzh.ddis.thesis.lambda_architecture.batch.cache;

import org.apache.samza.metrics.Counter;
import org.apache.samza.serializers.Serde;
import org.apache.samza.storage.kv.*;
import org.iq80.leveldb.Options;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.List;

/**
 * In order to test the samza storage functionality properly we have to wrap our implementation around
 * LevelDB. It's quite a dirty hack, but it should do the trick for now :)
 *
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public final class HashKV<K, V> implements KeyValueStore<K,V>{

    //private final Map<K, V> map = new HashMap<>();

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();


    private final LevelDbKeyValueStore map;
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;


    public HashKV(Serde<K> keySerde, Serde<V> valueSerde) throws IOException {
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        LevelDbKeyValueStoreMetrics metrics = Mockito.mock(LevelDbKeyValueStoreMetrics.class);
        Mockito.when(metrics.gets()).thenReturn(new Counter("test"));
        Mockito.when(metrics.puts()).thenReturn(new Counter("test"));
        Mockito.when(metrics.bytesWritten()).thenReturn(new Counter("test"));
        Mockito.when(metrics.bytesRead()).thenReturn(new Counter("test"));
        Mockito.when(metrics.ranges()).thenReturn(new Counter("ranges"));
        Mockito.when(metrics.deletes()).thenReturn(new Counter("deletes"));
        Mockito.when(metrics.alls()).thenReturn(new Counter("alls"));
        folder.create();
        this.map = new LevelDbKeyValueStore(folder.getRoot(), new Options(), 100, metrics);
    }


    @Override
    public V get(K k) {
        return valueSerde.fromBytes(map.get(keySerde.toBytes(k)));
    }

    @Override
    public void put(K k, V v) {
        map.put(keySerde.toBytes(k), valueSerde.toBytes(v));
    }

    @Override
    public void putAll(List<Entry<K, V>> entries) {
        for(Entry<K, V> entry : entries){
            map.put(keySerde.toBytes(entry.getKey()), valueSerde.toBytes(entry.getValue()));
        }
    }

    @Override
    public void delete(K k) {
        map.delete(keySerde.toBytes(k));
    }

    @Override
    public KeyValueIterator<K, V> range(K k, K k2) {
        return new LevelDBIt(map.range(keySerde.toBytes(k), keySerde.toBytes(k2)));
    }

    @Override
    public KeyValueIterator<K, V> all() {
        return new LevelDBIt(map.all());
    }

    @Override
    public void close() {
        map.close();
    }

    @Override
    public void flush() {
        map.flush();
    }


    private class LevelDBIt implements KeyValueIterator<K, V>{

        private final KeyValueIterator<byte[], byte[]> it;

        public LevelDBIt(KeyValueIterator<byte[], byte[]> it){
            this.it = it;
        }

        @Override
        public void close() {
            it.close();
        }

        @Override
        public boolean hasNext() {
            return it.hasNext();
        }

        @Override
        public Entry<K, V> next() {
            Entry<byte[], byte[]> entry = it.next();
            Entry<K, V> newEntry = new Entry(keySerde.fromBytes(entry.getKey()), valueSerde.fromBytes(entry.getValue()));
            return newEntry;
        }

        @Override
        public void remove() {
            it.remove();
        }
    }
}
