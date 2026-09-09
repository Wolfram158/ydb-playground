package ru.wolfram.playground

import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Service

@Service
class PlaygroundService(
    private val jdbcTemplate: JdbcTemplate
) {
    fun saveMessage(msg: String) {
        jdbcTemplate.update("insert into outbox(msg, status) values (?, ?)", msg, "pending")
    }
}