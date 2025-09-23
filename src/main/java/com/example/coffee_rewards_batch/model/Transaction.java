package com.example.coffee_rewards_batch.model;

import lombok.*;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ToString
@EqualsAndHashCode
public class Transaction {
    private String customerId;
    private LocalDateTime timestamp;
    private BigDecimal amount;
}
