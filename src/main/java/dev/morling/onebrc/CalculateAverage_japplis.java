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
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
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
 * - Use RandomAccessFile instead of FileInputStream
 * - Use ByteBuffer instead of Text (7" on laptop)
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

    private Map<ByteBuffer, IntSummaryStatistics> cityMeasurementMap = new ConcurrentHashMap<>(MAP_CAPACITY);
    private Map<String, List<Byte>> splitLines = new ConcurrentHashMap<>(MAP_CAPACITY);
    private Semaphore readFileLock = new Semaphore(MAX_COMPUTE_THREADS);

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
                ByteBuffer blockData = readBlock(blockIndex);
                int startIndex = readSplitLines(blockData, blockIndex);
                Map<ByteBuffer, IntSummaryStatistics> blockCityMeasurementMap = parseTemperaturesBlock(blockData, startIndex);
                mergeBlockResults(blockCityMeasurementMap);
            }
            catch (Exception ex) {
                ex.printStackTrace();
            }
            finally {
                readFileLock.release();
            }
        };
    }

    private ByteBuffer readBlock(long blockIndex) throws IOException {
        long fileIndex = blockIndex * BUFFER_SIZE;
        if (fileIndex >= fileSize) {
            return null;
        }
        try (RandomAccessFile measurementsFileRAF = new RandomAccessFile(measurementsFile, "r")) {
            long bufferSize = Math.min(BUFFER_SIZE, fileSize - fileIndex);
            ByteBuffer mappedBuffer = measurementsFileRAF.getChannel().map(FileChannel.MapMode.READ_ONLY, fileIndex, bufferSize);
            return mappedBuffer;
        }
    }

    private Map<ByteBuffer, IntSummaryStatistics> parseTemperaturesBlock(ByteBuffer buffer, int startIndex) {
        int bufferIndex = startIndex;
        Map<ByteBuffer, IntSummaryStatistics> blockCityMeasurementMap = new HashMap<>(MAP_CAPACITY);
        try {
            while (bufferIndex < buffer.limit()) {
                bufferIndex = readNextLine(bufferIndex, buffer, blockCityMeasurementMap);
            }
        }
        catch (IndexOutOfBoundsException ex) {
            // Done reading and parsing the buffer
        }

        return blockCityMeasurementMap;
    }

    private int readSplitLines(ByteBuffer buffer, int blockIndex) {
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

    private List<Byte> readStartSplitLine(ByteBuffer buffer) {
        List<Byte> splitLine = new ArrayList<>(100);
        int tailIndex = buffer.limit();
        byte car = buffer.get(--tailIndex);
        while (car != '\n') {
            splitLine.add(0, car);
            car = buffer.get(--tailIndex);
        }
        return splitLine;
    }

    private List<Byte> readEndSplitLine(ByteBuffer buffer) {
        List<Byte> splitLine = new ArrayList<>(100);
        int bufferIndex = 0;
        byte car = buffer.get(bufferIndex++);
        while (car != '\n') {
            splitLine.add(car);
            car = buffer.get(bufferIndex++);
        }
        splitLine.add((byte) '\n');
        return splitLine;
    }

    private void parseSplitLines() {
        for (int i = 1; i < totalBlocks; i++) {
            List<Byte> startSplitLine = splitLines.get("start-" + i);
            List<Byte> endSplitLine = splitLines.get("end-" + i);
            startSplitLine.addAll(endSplitLine);
            ByteBuffer splitLineBytes = ByteBuffer.allocate(startSplitLine.size());
            for (int j = 0; j < startSplitLine.size(); j++) {
                splitLineBytes.put(j, (byte) startSplitLine.get(j));
            }
            readNextLine(0, splitLineBytes, cityMeasurementMap);
        }
    }

    private int readNextLine(int bufferIndex, ByteBuffer buffer, Map<ByteBuffer, IntSummaryStatistics> blockCityMeasurementMap) {
        int startLineIndex = bufferIndex;
        bufferIndex++; // city is at least 1 character
        while (buffer.get(bufferIndex) != (byte) ';') {
            bufferIndex++;
        }
        // String city = new String(cityBytes, 0, bufferIndex - startLineIndex, StandardCharsets.UTF_8);
        ByteBuffer city = buffer.slice(startLineIndex, bufferIndex - startLineIndex);
        bufferIndex++; // skip ';'
        int temperature = readTemperature(buffer, bufferIndex);
        bufferIndex += getIndexOffset(temperature);
        addTemperature(city, temperature, blockCityMeasurementMap);
        return bufferIndex;
    }

    private int readTemperature(ByteBuffer buffer, int bufferIndex) {
        boolean negative = buffer.get(bufferIndex) == (byte) '-';
        if (negative) {
            bufferIndex++;
        }
        byte digit = buffer.get(bufferIndex++);
        int temperature = 0;
        while (digit != (byte) '\n') {
            temperature = temperature * 10 + (digit - (byte) '0');
            digit = buffer.get(bufferIndex++);
            if (digit == (byte) '.') { // Skip '.'
                digit = buffer.get(bufferIndex++);
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
            return fractionDigitCount + 4; // e.g. 10.0\n
        }
        if (temperature >= 0) {
            return fractionDigitCount + 3; // e.g. 1.0\n
        }
        if (temperature <= -tenDegreesInt) {
            return fractionDigitCount + 5; // e.g. -10.0\n
        }
        return fractionDigitCount + 4; // e.g. -1.0\n
    }

    private void addTemperature(ByteBuffer city, int temperature, Map<ByteBuffer, IntSummaryStatistics> blockCityMeasurementMap) {
        IntSummaryStatistics measurement = blockCityMeasurementMap.get(city);
        if (measurement == null) {
            measurement = new IntSummaryStatistics();
            blockCityMeasurementMap.put(city, measurement);
        }
        measurement.accept(temperature);
    }

    private void mergeBlockResults(Map<ByteBuffer, IntSummaryStatistics> blockCityMeasurementMap) {
        for (Map.Entry<ByteBuffer, IntSummaryStatistics> cityMeasurement : blockCityMeasurementMap.entrySet()) {
            cityMeasurementMap.merge(cityMeasurement.getKey(), cityMeasurement.getValue(), (m1, m2) -> {
                m1.combine(m2);
                return m1;
            });
        }
    }

    private void printTemperatureStatsByCity() {
        Map<String, IntSummaryStatistics> sortedCityMeasurement = new TreeMap<>();
        cityMeasurementMap.forEach((city, measument) -> sortedCityMeasurement.put(toString(city), measument));
        StringBuilder result = new StringBuilder(cityMeasurementMap.size() * 40);
        result.append('{');
        sortedCityMeasurement.forEach((city, measurement) -> {
            result.append(city);
            result.append(getTemperatureStats(measurement));
        });
        if (!sortedCityMeasurement.isEmpty()) {
            result.delete(result.length() - 2, result.length());
        }
        result.append('}');
        String temperaturesByCity = result.toString();
        System.out.println(temperaturesByCity);
    }

    private static String toString(ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.limit()];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = buffer.get(i);
        }
        String text = new String(bytes, StandardCharsets.UTF_8);
        return text;
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
}