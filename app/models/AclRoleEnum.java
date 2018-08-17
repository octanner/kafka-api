package models;

import org.apache.kafka.common.acl.AclOperation;

public enum AclRoleEnum {
    PRODUCER("WRITE"),
    CONSUMER("READ");

    private AclOperation operation;

    AclRoleEnum(String role) {
        this.operation = AclOperation.fromString(role);
    }

    public AclOperation getOperation() {
        return operation;
    }
}
