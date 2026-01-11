package dev.prpatel.iceberg.app;

import org.springframework.ai.chat.prompt.PromptTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
class LLMController {

    private final AiService aiService;

    @Autowired
    public LLMController(AiService aiService) {
        this.aiService = aiService;
    }

    @GetMapping("/ai/simple")
    public String simpleCompletion(@RequestParam(value = "message", defaultValue = "Tell me a joke") String message) {
        // .call() is the simplest way to get a response
        return aiService.getChatClient().prompt().user(message).call().content();
    }

    /**
     * An endpoint that uses a PromptTemplate for more structured interactions.
     */
    @GetMapping("/ai/prompt")
    public String promptWithRole(@RequestParam(value = "topic", defaultValue = "dogs") String topic) {
        String message = """
                Tell me a short, fun fact about {topic}. Your response should be brief,
                enthusiastic, and contain at least one emoji.
                """;

        PromptTemplate promptTemplate = new PromptTemplate(message);

        // The Map provides the value for the {topic} placeholder
        return aiService.getChatClient().prompt(promptTemplate.create(Map.of("topic", topic)))
                .call()
                .content();
    }

    @GetMapping("/ai/generateQuery")
    public String generateQuery(
            @RequestParam(value = "query", defaultValue = "show me all the properties sold in Clapham") String query) {
        return aiService.generateQuery(query);
    }
}
