package com.example.coffee_rewards_batch.model;

import lombok.*;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Getter
@Setter
@ToString
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class RewardEvent {
    private String customerId;
    private int points;
    private BigDecimal amount;
    private LocalDateTime timestamp;
}
