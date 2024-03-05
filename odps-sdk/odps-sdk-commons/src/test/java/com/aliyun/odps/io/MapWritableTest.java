package com.aliyun.odps.io;

import org.junit.Assert;
import org.junit.Test;

import java.util.*;

/**
 * Created by biliang.wbl on 23/11/17.
 */
public class MapWritableTest {
    @Test
    public void testSequence() {
        List<Pair<Integer, Integer>> list = buildRandomPairList();
        MapWritable mapContainer = new MapWritable();
        for (Pair<Integer, Integer> pair : list) {
            mapContainer.put(new IntWritable(pair.left), new IntWritable(pair.right));
        }
        Assert.assertTrue(checkSequence(list, mapContainer));
    }

    private boolean checkSequence(List<Pair<Integer, Integer>> list, MapWritable mapContainer) {
        List<Map.Entry<Writable, Writable>> entryList = new ArrayList<>(mapContainer.entrySet());
        if (entryList.size() != list.size()) {
            return false;
        }
        for (int i = 0, n = list.size(); i < n; i++) {
            Pair<Integer, Integer> pair = list.get(i);
            Map.Entry<Writable, Writable> entry = entryList.get(i);
            IntWritable key = (IntWritable) entry.getKey();
            IntWritable value = (IntWritable) entry.getValue();
            boolean eq = pair.left == key.get() && pair.right == value.get();
            if (!eq) {
                return false;
            }
        }
        return true;
    }

    private List<Pair<Integer, Integer>> buildRandomPairList() {
        Map<Integer, Integer> map = new HashMap<>();
        Random r = new Random();
        for (int i = 0, n = r.nextInt(1024); i < n; i++) {
            map.put(r.nextInt(), r.nextInt());
        }
        List<Pair<Integer, Integer>> list = new ArrayList<>();
        for (Map.Entry<Integer, Integer> entry : map.entrySet()) {
            list.add(new Pair<>(entry.getKey(), entry.getValue()));
        }
        Collections.shuffle(list);
        return list;
    }

    private static class Pair<L, R> {
        private L left;
        private R right;

        public Pair(L left, R right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Pair<?, ?> pair = (Pair<?, ?>) o;
            return Objects.equals(left, pair.left) && Objects.equals(right, pair.right);
        }

        @Override
        public int hashCode() {
            return Objects.hash(left, right);
        }
    }
}
