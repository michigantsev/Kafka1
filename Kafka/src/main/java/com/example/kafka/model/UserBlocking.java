package com.example.kafka.model;

import java.util.Set;

public class UserBlocking {
    public String userId;
    public Set<String> blockedUserId;
    public long timestamp;
}
