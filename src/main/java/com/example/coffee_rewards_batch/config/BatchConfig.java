package com.example.coffee_rewards_batch.config;

import com.example.coffee_rewards_batch.job.StepLoggingListener;
import com.example.coffee_rewards_batch.model.RewardEvent;
import com.example.coffee_rewards_batch.model.Transaction;
import com.example.coffee_rewards_batch.partition.RangePartitioner;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.LocalDateTime;

@Configuration
public class BatchConfig {
    @Bean
    @StepScope
    public FlatFileItemReader<Transaction> transactionReader(
            @Value("#{jobParameters['inputFile']}") String inputFile,
            @Value("#{stepExecutionContext['startLine']}") Integer startLine,
            @Value("#{stepExecutionContext['endLine']}") Integer endLine
    ) {
        return new FlatFileItemReaderBuilder<Transaction>()
                .name("transactionReader")
                .resource(new FileSystemResource(inputFile)) // use file path passed as job param
                .delimited()
                .names("customerId","timestamp","amount")
                .linesToSkip(startLine) // skip until startLine
                .maxItemCount(endLine - startLine + 1) // only read this partition
                .fieldSetMapper(fieldSet -> Transaction
                        .builder()
                        .customerId(fieldSet.readString("customerId"))
                        .timestamp(LocalDateTime.parse(fieldSet.readString("timestamp")))
                        .amount(new BigDecimal(fieldSet.readString("amount")))
                        .build())
                .build();
    }

    @Bean
    public ItemProcessor<Transaction, RewardEvent> rewardCalculator() {
        return tx -> {
            // Business rule: 1 point per 10 currency units
            int points = tx.getAmount().divide(new java.math.BigDecimal("10")).intValue();
            return RewardEvent
                    .builder()
                    .customerId(tx.getCustomerId())
                    .points(points)
                    .amount(tx.getAmount())
                    .timestamp(tx.getTimestamp())
                    .build();
        };
    }

    @Bean
    public JdbcBatchItemWriter<RewardEvent> rewardWriter(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<RewardEvent>()
                .sql("""
                 INSERT INTO reward_events (customer_id, points, tx_amount, tx_time)
                 VALUES (:customerId, :points, :amount, :timestamp)
                 """)
                .beanMapped()  // maps fields from RewardEvent by name
                .dataSource(dataSource)
                .build();
    }

    @Bean
    public TaskExecutor taskExecutor() {
        SimpleAsyncTaskExecutor executor = new SimpleAsyncTaskExecutor("batch-thread-");
        executor.setConcurrencyLimit(4);  // max 4 threads at once
        return executor;
    }

    @Bean
    public Step slaveStep(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            FlatFileItemReader<Transaction> reader,
            ItemProcessor<Transaction, RewardEvent> processor,
            JdbcBatchItemWriter<RewardEvent> writer) {

        return new StepBuilder("slaveStep", jobRepository)
                .<Transaction, RewardEvent>chunk(100, transactionManager)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .listener(new StepLoggingListener())

                // === Fault tolerance ===
                .faultTolerant()
                .skipLimit(10) // allow up to 10 bad records
                .skip(Exception.class) // skip any Exception
                .retryLimit(3) // retry up to 3 times
                .retry(org.springframework.dao.DeadlockLoserDataAccessException.class) // retry on DB deadlocks
                .build();
    }

    @Bean
    public PartitionHandler partitionHandler(Step slaveStep) {
        TaskExecutorPartitionHandler handler = new TaskExecutorPartitionHandler();
        handler.setStep(slaveStep);
        handler.setTaskExecutor(taskExecutor());
        handler.setGridSize(4); // number of partitions/threads
        return handler;
    }

    @Bean
    public Step masterStep(JobRepository jobRepository,
                           PlatformTransactionManager transactionManager,
                           Step slaveStep) {
        Path filePath = Path.of("src/main/resources/input/transactions.csv");
        
        return new StepBuilder("masterStep", jobRepository)
                .partitioner("slaveStep", new RangePartitioner(filePath, 250))
                .partitionHandler(partitionHandler(slaveStep))
                .build();
    }

    @Bean
    public Job importTransactionsJob(JobRepository jobRepository, Step masterStep) {
        return new JobBuilder("importTransactions", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(masterStep)
                .build();
    }
}
