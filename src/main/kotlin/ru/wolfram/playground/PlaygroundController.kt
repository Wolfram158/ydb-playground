package ru.wolfram.playground

import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RestController

@RestController
class PlaygroundController(
    private val service: PlaygroundService
) {
    @PostMapping("/send")
    fun sendMessage(msg: String) {
        service.saveMessage(msg)
    }
}