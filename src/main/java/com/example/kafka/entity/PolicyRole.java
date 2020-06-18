package com.example.kafka.entity;

import lombok.Getter;

import javax.persistence.Entity;
import javax.persistence.Id;

@Entity
@Getter
public class PolicyRole {
    @Id
    private Long id;

    private Long policyId;
}
