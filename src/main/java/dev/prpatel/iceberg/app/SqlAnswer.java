package dev.prpatel.iceberg.app;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * What the model is asked to return, instead of a bare string that may or may not be wrapped in
 * markdown fences.
 *
 * The shape is declared in the system prompt, not here, so it can be changed from /admin without a
 * rebuild. This class only says how to read it back. Asking for fields that can be checked is the
 * point: {@code columnsUsed} can be compared against {@link SchemaStore}, which catches a query
 * built on a column the model invented before it is ever executed.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record SqlAnswer(
        String sql,
        @JsonProperty("columns_used") List<String> columnsUsed,
        String assumptions) {

    public List<String> columnsUsed() {
        return columnsUsed == null ? List.of() : columnsUsed;
    }

    public String assumptions() {
        return assumptions == null ? "" : assumptions;
    }
}
