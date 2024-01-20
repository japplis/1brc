/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.onebrc;

import java.io.*;
import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;

/**
 * Maybe not the fastest but trying to get the most readable code for the performance.
 *
 * It allows:
 *  - pass another file as argument
 *  - the first lines can start with comments lines using '#'
 *  - the temperatures can have more than one fraction digit but it needs to be constant in the file
 *  - it does not require much RAM
 *  - Java 8 as minimal Java version
 * Assumptions
 *  - No temperatures are above 100 or below -100
 *  - the last character of the file is \n
 *
 * Changelog:
 * - First local attempt with FileReader and TreeMap: Way too long
 * - Switched to InputStream and ConcurrentHashMap: 23"
 * - Added Semaphore to avoid OOMException: 23"
 * - Replaced String with my own ByteText class: a bit slower (~10%)
 * - Replaced compute lambda call with synchronized(city.intern()): 43" (due to intern())
 * - Removed BufferedInputStream and replaced Measurement with IntSummaryStatistics (thanks davecom): still 23" but cleaner code
 * - Execute same code on 1BRC server: 41"
 * - One HashMap per thread: 17" locally (12" on 1BRC server)
 * - Read file in multiple threads if available and
 * - Changed String to (byte[]) Text with cache: 18" locally (but 8" -> 5" on laptop) (10" on 1BRC server)
 * - Changed Map.combine() to Map.merge() and Map.forEach() to 'for each' loop
 * - Replaced Map[Integer, Text] to IntObjectHashMap[Text] (5" on laptop) (11" on 1BRC server)
 * - Read and parsing block done in the same task
 * - Compare byte[] using MemorySegment
 *
 * Measure-Command { java -cp .\target\average-1.0.0-SNAPSHOT.jar dev.morling.onebrc.CalculateAverage_japplis }
 *
 * @author Anthony Goubard - Japplis
 */
public class CalculateAverage_japplis {

    private static final int MAP_CAPACITY = 10_000;
    private static final String DEFAULT_MEASUREMENT_FILE = "measurements.txt";
    private static final int BUFFER_SIZE = 5 * 1024 * 1024; // 5 MB
    private static final int MAX_COMPUTE_THREADS = Runtime.getRuntime().availableProcessors();

    private int fractionDigitCount;
    private int tenDegreesInt;
    private File measurementsFile;
    private long fileSize;
    private int totalBlocks;

    private Map<Text, IntSummaryStatistics> cityMeasurementMap = new ConcurrentHashMap<>(MAP_CAPACITY);
    private Map<String, List<Byte>> splitLines = new ConcurrentHashMap<>(MAP_CAPACITY);
    private Semaphore readFileLock = new Semaphore(MAX_COMPUTE_THREADS);
    private Queue<ByteArray> bufferPool = new ConcurrentLinkedQueue<>();
    private Queue<IntObjectHashMap<Text>> textPoolPool = new ConcurrentLinkedQueue<>();

    private CalculateAverage_japplis(File measurementsFile, int fractionDigitCount) {
        this.measurementsFile = measurementsFile;
        this.fractionDigitCount = fractionDigitCount;
        tenDegreesInt = (int) Math.pow(10, fractionDigitCount) * 10;
        fileSize = measurementsFile.length();
        totalBlocks = (int) (fileSize / BUFFER_SIZE) + 1;
    }

    private void parseTemperatures() throws Exception {
        int blockIndex = 0;
        ExecutorService threadPool = Executors.newFixedThreadPool(MAX_COMPUTE_THREADS);
        List<Future> readAndParseBlockTasks = new ArrayList<>();

        while (blockIndex < totalBlocks) {
            readFileLock.acquire();
            Runnable blockReaderParser = readAndParseBlock(blockIndex);
            Future readAndParseBlockTask = threadPool.submit(blockReaderParser);
            readAndParseBlockTasks.add(readAndParseBlockTask);
            blockIndex++;
        }
        for (Future readAndParseBlockTask : readAndParseBlockTasks) { // Wait for all tasks to finish
            readAndParseBlockTask.get();
        }
        parseSplitLines();
        threadPool.shutdownNow();
    }

    private Runnable readAndParseBlock(int blockIndex) {
        return () -> {
            try {
                ByteArray blockData = readBlock(blockIndex);
                int startIndex = readSplitLines(blockData, blockIndex);
                Map<Text, IntSummaryStatistics> blockCityMeasurementMap = parseTemperaturesBlock(blockData, startIndex);
                mergeBlockResults(blockCityMeasurementMap);
            }
            catch (Exception ex) {
            }
            finally {
                readFileLock.release();
            }
        };
    }

    private ByteArray readBlock(long blockIndex) throws IOException {
        long fileIndex = blockIndex * BUFFER_SIZE;
        if (fileIndex >= fileSize) {
            return new ByteArray(0);
        }
        try (InputStream measurementsFileIS = new FileInputStream(measurementsFile)) {
            if (fileIndex > 0) {
                long skipped = measurementsFileIS.skip(fileIndex);
                while (skipped != fileIndex) {
                    skipped += measurementsFileIS.skip(fileIndex - skipped);
                }
            }
            long bufferSize = Math.min(BUFFER_SIZE, fileSize - fileIndex);
            ByteArray buffer = bufferSize == BUFFER_SIZE ? bufferPool.poll() : new ByteArray((int) bufferSize);
            if (buffer == null) {
                buffer = new ByteArray(BUFFER_SIZE);
            }
            int totalRead = measurementsFileIS.read(buffer.array(), 0, (int) bufferSize);
            while (totalRead < bufferSize) {
                byte[] extraBuffer = new byte[(int) (bufferSize - totalRead)];
                int readCount = measurementsFileIS.read(extraBuffer);
                System.arraycopy(extraBuffer, 0, buffer.array(), totalRead, readCount);
                totalRead += readCount;
            }
            return buffer;
        }
    }

    private Map<Text, IntSummaryStatistics> parseTemperaturesBlock(ByteArray buffer, int startIndex) {
        int bufferIndex = startIndex;
        Map<Text, IntSummaryStatistics> blockCityMeasurementMap = new HashMap<>(MAP_CAPACITY);
        IntObjectHashMap<Text> textPool = textPoolPool.poll();
        if (textPool == null) {
            textPool = new IntObjectHashMap<>(MAP_CAPACITY);
        }
        byte[] bufferArray = buffer.array();
        try {
            while (bufferIndex < bufferArray.length) {
                bufferIndex = readNextLine(bufferIndex, bufferArray, blockCityMeasurementMap, textPool);
            }
        }
        catch (ArrayIndexOutOfBoundsException ex) {
            // Done reading and parsing the buffer
        }
        if (bufferArray.length == BUFFER_SIZE) {
            bufferPool.add(buffer);
            textPoolPool.add(textPool);
        }
        return blockCityMeasurementMap;
    }

    private int readSplitLines(ByteArray blockData, int blockIndex) {
        byte[] buffer = blockData.array();
        int startIndex = 0;
        if (blockIndex < totalBlocks - 1) {
            List<Byte> startSplitLine = readStartSplitLine(buffer); // Last line of the block
            splitLines.put("start-" + (blockIndex + 1), startSplitLine);
        }
        if (blockIndex > 0) {
            List<Byte> endSplitLine = readEndSplitLine(buffer); // First line of the block
            splitLines.put("end-" + blockIndex, endSplitLine);
            startIndex = endSplitLine.size();
        }
        return startIndex;
    }

    private List<Byte> readEndSplitLine(byte[] buffer) {
        List<Byte> splitLine = new ArrayList<>(100);
        int bufferIndex = 0;
        byte car = buffer[bufferIndex++];
        while (car != '\n') {
            splitLine.add(car);
            car = buffer[bufferIndex++];
        }
        splitLine.add((byte) '\n');
        return splitLine;
    }

    private List<Byte> readStartSplitLine(byte[] buffer) {
        List<Byte> splitLine = new ArrayList<>(100);
        int tailIndex = buffer.length;
        byte car = buffer[--tailIndex];
        while (car != '\n') {
            splitLine.add(0, car);
            car = buffer[--tailIndex];
        }
        return splitLine;
    }

    private void parseSplitLines() {
        IntObjectHashMap<Text> textPool = textPoolPool.poll();
        if (textPool == null) {
            textPool = new IntObjectHashMap<>(totalBlocks);
        }
        for (int i = 1; i < totalBlocks; i++) {
            List<Byte> startSplitLine = splitLines.get("start-" + i);
            List<Byte> endSplitLine = splitLines.get("end-" + i);
            startSplitLine.addAll(endSplitLine);
            byte[] splitLineBytes = new byte[startSplitLine.size()];
            for (int j = 0; j < splitLineBytes.length; j++) {
                splitLineBytes[i] = startSplitLine.get(i);
            }
            readNextLine(0, splitLineBytes, cityMeasurementMap, textPool);
        }
    }

    private int readNextLine(int bufferIndex, byte[] buffer, Map<Text, IntSummaryStatistics> blockCityMeasurementMap, IntObjectHashMap<Text> textPool) {
        int startLineIndex = bufferIndex;
        while (buffer[bufferIndex] != (byte) ';') {
            bufferIndex++;
        }
        // String city = new String(buffer, startLineIndex, bufferIndex - startLineIndex, StandardCharsets.UTF_8);
        Text city = Text.getByteText(buffer, startLineIndex, bufferIndex - startLineIndex, textPool);
        bufferIndex++; // skip ';'
        int temperature = readTemperature(buffer, bufferIndex);
        bufferIndex += getIndexOffset(temperature);
        addTemperature(city, temperature, blockCityMeasurementMap);
        return bufferIndex;
    }

    private int readTemperature(byte[] buffer, int bufferIndex) {
        boolean negative = buffer[bufferIndex] == (byte) '-';
        if (negative) {
            bufferIndex++;
        }
        byte digit = buffer[bufferIndex++];
        int temperature = 0;
        while (digit != (byte) '\n') {
            temperature = temperature * 10 + (digit - (byte) '0');
            digit = buffer[bufferIndex++];
            if (digit == (byte) '.') { // Skip '.'
                digit = buffer[bufferIndex++];
            }
        }
        if (negative) {
            temperature = -temperature;
        }
        return temperature;
    }

    private int getIndexOffset(int temperature) {
        // offset is at least fractionDigitCount + 3 (digit, dot and LF)
        if (temperature >= tenDegreesInt) {
            return fractionDigitCount + 4;
        }
        if (temperature >= 0) {
            return fractionDigitCount + 3;
        }
        if (temperature <= -tenDegreesInt) {
            return fractionDigitCount + 5;
        }
        return fractionDigitCount + 4;
    }

    private void addTemperature(Text city, int temperature, Map<Text, IntSummaryStatistics> blockCityMeasurementMap) {
        IntSummaryStatistics measurement = blockCityMeasurementMap.get(city);
        if (measurement == null) {
            measurement = new IntSummaryStatistics();
            blockCityMeasurementMap.put(city, measurement);
        }
        measurement.accept(temperature);
    }

    private void mergeBlockResults(Map<Text, IntSummaryStatistics> blockCityMeasurementMap) {
        for (Map.Entry<Text, IntSummaryStatistics> cityMeasurement : blockCityMeasurementMap.entrySet()) {
            cityMeasurementMap.merge(cityMeasurement.getKey(), cityMeasurement.getValue(), (m1, m2) -> {
                m1.combine(m2);
                return m1;
            });
        }
    }

    private void printTemperatureStatsByCity() {
        Set<Text> sortedCities = new TreeSet<>(cityMeasurementMap.keySet());
        StringBuilder result = new StringBuilder(cityMeasurementMap.size() * 40);
        result.append('{');
        sortedCities.forEach(city -> {
            IntSummaryStatistics measurement = cityMeasurementMap.get(city);
            result.append(city);
            result.append(getTemperatureStats(measurement));
        });
        if (!sortedCities.isEmpty()) {
            result.delete(result.length() - 2, result.length());
        }
        result.append('}');
        String temperaturesByCity = result.toString();
        System.out.println(temperaturesByCity);
    }

    private String getTemperatureStats(IntSummaryStatistics measurement) {
        StringBuilder stats = new StringBuilder(19);
        stats.append('=');
        appendTemperature(stats, measurement.getMin());
        stats.append('/');
        int average = (int) Math.round(measurement.getAverage());
        appendTemperature(stats, average);
        stats.append('/');
        appendTemperature(stats, measurement.getMax());
        stats.append(", ");
        return stats.toString();
    }

    private void appendTemperature(StringBuilder resultBuilder, int temperature) {
        String temperatureAsText = String.valueOf(temperature);
        int minCharacters = fractionDigitCount + (temperature < 0 ? 2 : 1);
        for (int i = temperatureAsText.length(); i < minCharacters; i++) {
            temperatureAsText = temperature < 0 ? "-0" + temperatureAsText.substring(1) : "0" + temperatureAsText;
        }
        int dotPosition = temperatureAsText.length() - fractionDigitCount;
        resultBuilder.append(temperatureAsText.substring(0, dotPosition));
        resultBuilder.append('.');
        resultBuilder.append(temperatureAsText.substring(dotPosition));
    }

    public static final void main(String... args) throws Exception {
        String measurementFile = args.length == 1 ? args[0] : DEFAULT_MEASUREMENT_FILE;
        CalculateAverage_japplis cityTemperaturesCalculator = new CalculateAverage_japplis(new File(measurementFile), 1);
        cityTemperaturesCalculator.parseTemperatures();
        cityTemperaturesCalculator.printTemperatureStatsByCity();
    }

    private class ByteArray {

        private final byte[] array;

        private ByteArray(int size) {
            array = new byte[size];
        }

        private byte[] array() {
            return array;
        }
    }

    private static class Text implements Comparable<Text> {

        private final byte[] textBytes;
        private final int hash;
        private String text;

        private Text(byte[] buffer, int startIndex, int length, int hash) {
            textBytes = new byte[length];
            this.hash = hash;
            System.arraycopy(buffer, startIndex, textBytes, 0, length);
        }

        private static Text getByteText(byte[] buffer, int startIndex, int length, IntObjectHashMap<Text> textPool) {
            int hash = hashCode(buffer, startIndex, length);
            Text textFromPool = textPool.get(hash);
            if (textFromPool == null || !arrayEquals(buffer, startIndex, length, textFromPool.textBytes)) {
                Text newText = new Text(buffer, startIndex, length, hash);
                textPool.put(hash, newText);
                return newText;
            }
            return textFromPool;
        }

        private static int hashCode(byte[] buffer, int startIndex, int length) {
            int hash = 31;
            int endIndex = startIndex + length;
            for (int i = startIndex; i < endIndex; i++) {
                hash = 31 * hash + buffer[i];
            }
            return hash;
        }

        private static boolean arrayEquals(byte[] array, int offset, int length, byte[] other) {
            MemorySegment arraySegment = MemorySegment.ofArray(array).asSlice(offset, length);
            MemorySegment otherSegment = MemorySegment.ofArray(other);
            return otherSegment.mismatch(arraySegment) == 0L;
        }

        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public boolean equals(Object other) {
            return other != null &&
                    hashCode() == other.hashCode() &&
                    other instanceof Text &&
                    Arrays.equals(textBytes, ((Text) other).textBytes);
        }

        @Override
        public int compareTo(Text other) {
            return toString().compareTo(other.toString());
        }

        @Override
        public String toString() {
            if (text == null) {
                text = new String(textBytes, StandardCharsets.UTF_8);
            }
            return text;
        }
    }

    // inspiration from https://javaexplorer03.blogspot.com/2015/10/create-own-hashmap.html
    private static class IntObjectHashMap<V> extends AbstractMap<Integer, V> {
        private static class IntEntry<V> implements Map.Entry<Integer, V> {
            private int key;
            private V value;
            private IntEntry next;

            private IntEntry(int key, V value) {
                this.key = key;
                this.value = value;
            }

            public int getIntKey() {
                return key;
            }

            @Override
            public Integer getKey() {
                return key;
            }

            @Override
            public V getValue() {
                return value;
            }

            @Override
            public V setValue(V value) {
                V oldValue = this.value;
                this.value = value;
                return oldValue;
            }
        }

        private IntEntry<V>[] entries;
        private int size;

        public IntObjectHashMap(int capacity) {
            entries = new IntEntry[capacity];
        }

        @Override
        public Set<Map.Entry<Integer, V>> entrySet() {
            Set<Map.Entry<Integer, V>> entrySet = new HashSet<>(entries.length);
            for (IntEntry entry : this.entries) {
                if (entry != null) {
                    entrySet.add(entry);
                    while (entry.next != null) {
                        entry = entry.next;
                        entrySet.add(entry);
                    }
                }
            }
            return entrySet;
        }

        public V put(int key, V value) {
            int location = key < 0 ? (-key % entries.length) : (key % entries.length);
            IntEntry<V> entry = entries[location];
            if (entry == null) {
                entries[location] = new IntEntry<>(key, value);
                size++;
                return null;
            }
            while (entry.next != null && entry.key != key) {
                entry = entry.next;
            }
            if (entry.key == key) {
                V oldValue = entry.setValue(value);
                return oldValue;
            }
            entry.next = new IntEntry<>(key, value);
            size++;
            return null;
        }

        public V get(int key) {
            int location = key < 0 ? (-key % entries.length) : (key % entries.length);
            IntEntry<V> entry = entries[location];
            if (entry == null) {
                return null;
            }
            while (entry.next != null && entry.key != key) {
                entry = entry.next;
            }
            if (entry.key == key) {
                return entry.getValue();
            }
            return null;
        }

        @Override
        public V put(Integer key, V value) {
            return put(key.intValue(), value);
        }

        @Override
        public V get(Object key) {
            return get(((Integer) key).intValue());
        }

        @Override
        public void clear() {
            entries = new IntEntry[entries.length];
            size = 0;
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public V remove(Object key) {
            throw new UnsupportedOperationException("Removing item is not supported");
        }
    }
}