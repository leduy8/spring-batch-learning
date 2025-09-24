package com.example.coffee_rewards_batch.job;

import lombok.AllArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

@Component
@AllArgsConstructor
public class JobScheduler {

    private final JobLauncher jobLauncher;
    private final Job importTransactionsJob;

    // Run daily at 2:00 AM for yesterday data
    @Scheduled(cron = "0 0 2 * * *")
    public void runJob() throws Exception {
        LocalDate yesterday = LocalDate.now().minusDays(1);
        
        String fileName = String.format("src/main/resources/input/transactions-%s", yesterday.format(DateTimeFormatter.ISO_DATE)); 

        JobParameters params = new JobParametersBuilder()
                .addString("inputFile", fileName)
                .addString("run.id", LocalDate.now().format(DateTimeFormatter.ISO_DATE)) // Unique per day
                .toJobParameters();

        jobLauncher.run(importTransactionsJob, params);
    }
}
