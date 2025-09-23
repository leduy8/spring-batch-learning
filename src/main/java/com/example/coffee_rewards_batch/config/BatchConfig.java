package com.example.coffee_rewards_batch.config;

import com.example.coffee_rewards_batch.model.RewardEvent;
import com.example.coffee_rewards_batch.model.Transaction;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
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
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.time.LocalDateTime;

@Configuration
public class BatchConfig {
    @Bean
    @StepScope
    public FlatFileItemReader<Transaction> transactionReader(@Value("#{jobParameters['inputFile']}") String inputFile) {
        return new FlatFileItemReaderBuilder<Transaction>()
                .name("transactionReader")
                .resource(new FileSystemResource(inputFile)) // use file path passed as job param
                .delimited()
                .names("customerId","timestamp","amount")
                .linesToSkip(1) // skip CSV header
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
    public Step processTransactionsStep(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            FlatFileItemReader<Transaction> reader,
            ItemProcessor<Transaction, RewardEvent> processor,
            JdbcBatchItemWriter<RewardEvent> writer) {

        return new StepBuilder("processTransactions", jobRepository)
                .<Transaction, RewardEvent>chunk(100, transactionManager)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .build();
    }

    @Bean
    public Job importTransactionsJob(JobRepository jobRepository, Step processTransactionsStep) {
        return new JobBuilder("importTransactions", jobRepository)
                .incrementer(new RunIdIncrementer()) // ensures unique job instance per run
                .start(processTransactionsStep)
                .build();
    }
}
