package dev.prpatel.iceberg.app;

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

    // Kept so the per-request options below can carry the same settings the defaults do.
    private final boolean useChatTemplateKwargs;
    private final String reasoningEffort;

    public AiService(ChatClient.Builder chatClientBuilder,
                     PromptStore promptStore,
                     ModelStore modelStore,
                     @Value("${app.ai.reasoning-effort:}") String reasoningEffort,
                     @Value("${app.ai.use-chat-template-kwargs:false}") boolean useChatTemplateKwargs) {

        this.promptStore = promptStore;
        this.modelStore = modelStore;
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
    public String generateQuery( String question) {

        // The system prompt sets the context and rules for the AI. It lives outside the code -
        // see PromptStore - so it can be edited from /admin without a rebuild.
        String systemPrompt = promptStore.get();
        System.out.println("system prompt (" + (promptStore.isCustomised() ? "edited" : "packaged default") + "):\n" + systemPrompt);

        String userPrompt = String.format(
                "table columns: \n %s \n" +
                        "User's question:\n%s",
                fieldsInData, question);

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
        return llmResponse.getResult().getOutput().getText();
    }

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

    private final String fieldsInData = """
Schema: table {
  1: transaction_id: required string (A reference number which is generated automatically recording each published sale. The number is unique and will change each time a sale is recorded.) (id)
  2: price: required int (Sale price stated on the transfer deed.)
  3: date_of_transfer: required date (Date when the sale was completed, as stated on the transfer deed.) Date is in YYYY-MM-DD format: YEAR-MONTH-DAY.
  4: postcode: required string (This is the postcode used at the time of the original transaction. Note that postcodes can be reallocated and these changes are not reflected in the Price Paid Dataset.)
  5: property_type: required string (D = Detached, S = Semi-Detached, T = Terraced, F = Flats/Maisonettes, O = Other)
  6: new_property: required string (Indicates the age of the property and applies to all price paid transactions, residential and non-residential. Y = a newly built property, N = an established residential building)
  7: duration: required string (Relates to the tenure: F = Freehold, L= Leasehold etc. Note that HM Land Registry does not record leases of 7 years or less in the Price Paid Dataset.)
  8: paon: optional string (Primary Addressable Object Name. Typically the house number or name)
  9: saon: optional string (Secondary Addressable Object Name. Where a property has been divided into separate units (for example, flats), the PAON (above) will identify the building and a SAON will be specified that identifies the separate unit/flat.)
  10: street: optional string
  11: locality: optional string
  12: town: optional string
  13: district: optional string
  14: county: optional string
  15: ppd_category_type: optional string (Indicates the type of Price Paid transaction. A = Standard Price Paid entry, includes single residential property sold for value. B = Additional Price Paid entry including transfers under a power of sale/repossessions, buy-to-lets (where they can be identified by a Mortgage), transfers to non-private individuals and sales where the property type is classed as ‘Other’.)
  16: record_status: optional string (Indicates additions, changes and deletions to the records. A = Addition C = Change D = Delete)
}
            """;

}
