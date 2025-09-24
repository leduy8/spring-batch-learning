package com.example.coffee_rewards_batch.job;

import org.springframework.batch.core.SkipListener;
import org.springframework.stereotype.Component;

@Component
public class StepLoggingListener implements SkipListener<Object, Object> {
    @Override
    public void onSkipInRead(Throwable t) {
        System.err.println("Skipped during read: " + t.getMessage());
    }

    @Override
    public void onSkipInWrite(Object item, Throwable t) {
        System.err.println("Skipped during write: " + item + " because " + t.getMessage());
    }

    @Override
    public void onSkipInProcess(Object item, Throwable t) {
        System.err.println("Skipped during process: " + item + " because " + t.getMessage());
    }
}
