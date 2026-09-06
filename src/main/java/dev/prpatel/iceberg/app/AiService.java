package dev.prpatel.iceberg.app;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.chat.model.ChatResponse;
import org.springframework.ai.openai.OpenAiChatOptions;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
//@Qualifier("ollamaChatModel")
class AiService {


    private final ChatClient chatClient;
    private final PromptStore promptStore;
    private final ModelStore modelStore;
    private final SchemaStore schemaStore;

    // Kept so the per-request options below can carry the same settings the defaults do.
    private final boolean useChatTemplateKwargs;
    private final String reasoningEffort;

    public AiService(ChatClient.Builder chatClientBuilder,
                     PromptStore promptStore,
                     ModelStore modelStore,
                     SchemaStore schemaStore,
                     @Value("${app.ai.reasoning-effort:}") String reasoningEffort,
                     @Value("${app.ai.use-chat-template-kwargs:false}") boolean useChatTemplateKwargs) {

        this.promptStore = promptStore;
        this.modelStore = modelStore;
        this.schemaStore = schemaStore;
        this.reasoningEffort = reasoningEffort;
        this.useChatTemplateKwargs = useChatTemplateKwargs;

        OpenAiChatOptions.Builder options = OpenAiChatOptions.builder();

        // Only the local server understands this, and the HuggingFace router rejects the whole
        // request with a 400 if it is present, so it has to be switched with the endpoint. It
        // lives here rather than in application.properties because it needs a real JSON boolean,
        // which a properties file cannot produce. The flag guarding it is an ordinary boolean,
        // so that much does live in application.properties, alongside the endpoint it belongs to.
        if (useChatTemplateKwargs) {
            options.extraBody(Map.of("chat_template_kwargs", Map.of("enable_thinking", false)));
        }

        this.chatClient = chatClientBuilder.defaultOptions(options).build();
    }

    public ChatClient getChatClient() {
        return chatClient;
    }
    public SqlAnswer generateAnswer(String question) {

        // The system prompt sets the context and rules for the AI. It lives outside the code -
        // see PromptStore - so it can be edited from /admin without a rebuild.
        String systemPrompt = promptStore.get();
        System.out.println("system prompt (" + (promptStore.isCustomised() ? "edited" : "packaged default") + "):\n" + systemPrompt);

        String userPrompt = String.format(
                "table columns: \n %s \n" +
                        "User's question:\n%s",
                schemaStore.get(), question);

        System.out.println(userPrompt);

        // The model is chosen per request rather than pinned at startup, so switching it on the
        // admin page takes effect immediately. Temperature comes from the client defaults, which
        // runtime options merge over rather than replace.
        String model = modelStore.get();
        OpenAiChatOptions.Builder perRequest = OpenAiChatOptions.builder().model(model);
        if (reasoningEffort != null && !reasoningEffort.isBlank()) {
            perRequest.reasoningEffort(reasoningEffort);
        }
        if (useChatTemplateKwargs) {
            perRequest.extraBody(Map.of("chat_template_kwargs", Map.of("enable_thinking", false)));
        }
        System.out.println("model: " + model + (modelStore.isCustomised() ? " (chosen on /admin)" : " (from application.properties)"));

        long startedAt = System.currentTimeMillis();
        ChatResponse llmResponse;
        try {
            llmResponse = ask(perRequest, systemPrompt, userPrompt);
        } catch (RuntimeException e) {
            // reasoning_effort is not portable. `none` is fine on some providers and returns
            // 400 "must be one of low, medium, or high" on others, so switching models would
            // otherwise fail for reasons that have nothing to do with the model. Drop it and
            // retry once rather than making the user care.
            if (!mentionsReasoningEffort(e)) {
                throw e;
            }
            System.out.println("note: " + model + " rejected reasoning_effort; retrying without it");
            OpenAiChatOptions.Builder withoutEffort = OpenAiChatOptions.builder().model(model);
            if (useChatTemplateKwargs) {
                withoutEffort.extraBody(Map.of("chat_template_kwargs", Map.of("enable_thinking", false)));
            }
            llmResponse = ask(withoutEffort, systemPrompt, userPrompt);
        }
        System.out.println("latency: " + (System.currentTimeMillis() - startedAt) + " ms");
        System.out.println("Response metadata: \n"+llmResponse.getResult().getMetadata());
        System.out.println("Response getOutput().getText: \n"+llmResponse.getResult().getOutput().getText());
        System.out.println("Response getOutput().toString: \n"+llmResponse.getResult().getOutput().toString());

        return parse(llmResponse.getResult().getOutput().getText());
    }

    /**
     * Read the model's reply as the JSON the system prompt asked for.
     *
     * Deliberately strict. Nothing here strips markdown fences or hunts for a JSON object inside
     * a longer answer: a model that ignores the contract is a prompt problem, and repairing it in
     * code would hide the very thing worth seeing. The raw reply is carried on the exception so the
     * page can show what actually came back.
     */
    private SqlAnswer parse(String raw) {
        try {
            SqlAnswer answer = MAPPER.readValue(raw, SqlAnswer.class);
            if (answer.sql() == null || answer.sql().isBlank()) {
                throw new UnstructuredResponseException(raw, "the JSON had no \"sql\" field");
            }
            return answer;
        } catch (UnstructuredResponseException e) {
            throw e;
        } catch (Exception e) {
            throw new UnstructuredResponseException(raw, e.getMessage());
        }
    }

    /** The model did not reply with the JSON the system prompt specified. */
    public static class UnstructuredResponseException extends RuntimeException {
        private final String raw;

        UnstructuredResponseException(String raw, String why) {
            super("The model did not return the JSON the system prompt asked for (" + why + ").");
            this.raw = raw;
        }

        /** Exactly what came back, so it can be shown rather than guessed at. */
        public String getRaw() {
            return raw;
        }
    }

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private ChatResponse ask(OpenAiChatOptions.Builder options, String systemPrompt, String userPrompt) {
        return chatClient.prompt()
                .options(options)
                .system(systemPrompt) // Apply the system role
                .user(userPrompt)     // Provide the user's request
                .call()
                .chatResponse();
    }

    /** True when a failure is the provider complaining about reasoning_effort, at any depth. */
    private static boolean mentionsReasoningEffort(Throwable t) {
        for (Throwable c = t; c != null; c = c.getCause()) {
            String m = c.getMessage();
            if (m != null && m.contains("reasoning_effort")) {
                return true;
            }
            if (c.getCause() == c) {
                break;
            }
        }
        return false;
    }
}
