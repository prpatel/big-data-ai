package dev.prpatel.iceberg.app;

import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.chat.model.ChatResponse;
import org.springframework.stereotype.Service;

@Service
//@Qualifier("ollamaChatModel")
class AiService {


    private final ChatClient chatClient;

    public AiService(ChatClient.Builder chatClientBuilder) {
        this.chatClient = chatClientBuilder.build();
    }

    public ChatClient getChatClient() {
        return chatClient;
    }
    public String generateQuery( String question) {

        // The system prompt sets the context and rules for the AI.
        String systemPrompt = """
                /no_think
                no_think
                You are a database query assistant. You are going to take a user's question and construct a 
                Spark SQL query from the question.
                Don't give me the explanation just give me the query. 
                I don't want the output to be escaped.
                The table name is: lakekeeper.housing.staging_prices.
                Make sure the the query accommodates for case insensitivity.
                I want to return all the results.
                The query should only include columns that are in the table. The table has these indicated in the user prompt.
                
                """;

        String userPrompt = String.format(
                "table columns: \n %s \n" +
                        "User's question:\n%s",
                fieldsInData, question);

        System.out.println(userPrompt);
        ChatResponse llmResponse = chatClient.prompt()
                .system(systemPrompt) // Apply the system role
                .user(userPrompt)     // Provide the user's request
                .call()
                .chatResponse();
        System.out.println("Response metadata: \n"+llmResponse.getResult().getMetadata());
        System.out.println("Response getOutput().getText: \n"+llmResponse.getResult().getOutput().getText());
        System.out.println("Response getOutput().toString: \n"+llmResponse.getResult().getOutput().toString());
        return llmResponse.getResult().getOutput().getText();
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
