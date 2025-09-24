package com.example.coffee_rewards_batch.partition;

import lombok.AllArgsConstructor;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

@AllArgsConstructor
public class RangePartitioner implements Partitioner {

    private final Path filePath;
    private final int partitionSize;

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        Map<String, ExecutionContext> result = new HashMap<>();

        long totalLines = countLines(filePath);
        if (totalLines <= 1) {
            // No data or only header
            return result;
        }

        long dataLines = totalLines - 1; // exclude header
        int partitions = (int) Math.ceil((double) dataLines / partitionSize);

        int start = 1; // skip header line
        for (int i = 0; i < partitions; i++) {
            int end = (int) Math.min(start + partitionSize - 1, dataLines);

            ExecutionContext context = new ExecutionContext();
            context.putInt("startLine", start + 1); // actual data start (skip header)
            context.putInt("endLine", end + 1);     // adjust for header

            result.put("partition" + i, context);

            start += partitionSize;
        }

        return result;
    }

    private long countLines(Path path) {
        try {
            return Files.lines(path).count();
        } catch (IOException e) {
            throw new RuntimeException("Failed to count lines in file: " + path, e);
        }
    }
}
