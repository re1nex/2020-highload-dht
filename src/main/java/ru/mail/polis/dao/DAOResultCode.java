package ru.mail.polis.dao;

public enum DAOResultCode {
    NOT_FOUND("Not found");

    private final String status;

    /**
     * Provide rc for dao.
     */
    DAOResultCode(final String status) {
        this.status = status;
    }
}
