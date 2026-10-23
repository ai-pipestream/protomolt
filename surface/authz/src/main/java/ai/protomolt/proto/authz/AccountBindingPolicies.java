package ai.protomolt.proto.authz;

import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.validate.ValidationResult;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Account-binding document preconditions and the boot file loader. The document's own
 * declared rules must hold, principal names must be unique, and no ACL identity may repeat
 * within a binding (document access rules match identities case-insensitively, so case
 * variants are the same identity) or claim the {@code public} type, which every caller
 * already matches. An intake writer scope must name one of its binding's accounts, and no
 * owner (account, datasource, connector) may be listed twice. A document failing here
 * refuses loudly naming the defect.
 */
public final class AccountBindingPolicies {

    /** Environment variable naming the binding file read at boot: {@value}. */
    public static final String ENV_ACCOUNT_BINDINGS = "PROTOMOLT_ACCOUNT_BINDINGS";

    private AccountBindingPolicies() {
    }

    /**
     * Refuses an invalid or ambiguous binding document.
     *
     * @param document the document to check
     * @return the document, for chaining
     */
    public static AccountBindings requireWellFormed(AccountBindings document) {
        if (document == null) {
            throw new IllegalArgumentException("account bindings must not be null");
        }
        ValidationResult rules = ProtoValidator.create().validate(document);
        if (!rules.valid()) {
            throw new IllegalArgumentException("account bindings are invalid: "
                    + rules.violations().stream()
                            .map(violation -> violation.path() + ": " + violation.message())
                            .collect(Collectors.joining("; ")));
        }
        Set<String> principals = new HashSet<>();
        for (AccountBinding binding : document.getBindingsList()) {
            if (!principals.add(binding.getPrincipal())) {
                throw new IllegalArgumentException("account bindings duplicate principal '"
                        + binding.getPrincipal() + "'");
            }
            Set<String> identities = new HashSet<>();
            for (AclIdentity identity : binding.getAclIdentitiesList()) {
                if ("public".equals(identity.getIdentityType())) {
                    throw new IllegalArgumentException("principal '" + binding.getPrincipal()
                            + "' binds the 'public' identity type, which every caller"
                            + " already matches");
                }
                String key = identity.getIdentityType() + " "
                        + identity.getIdentity().toLowerCase(Locale.ROOT);
                if (!identities.add(key)) {
                    throw new IllegalArgumentException("principal '" + binding.getPrincipal()
                            + "' binds ACL identity " + identity.getIdentityType() + " '"
                            + identity.getIdentity() + "' twice");
                }
            }
            Set<String> writers = new HashSet<>();
            for (IntakeWriterScope writer : binding.getIntakeWritersList()) {
                if (!binding.getAccountIdsList().contains(writer.getAccountId())) {
                    throw new IllegalArgumentException("principal '" + binding.getPrincipal()
                            + "' writes as datasource '" + writer.getDatasourceId() + "' in account '"
                            + writer.getAccountId() + "', which it is not bound to");
                }
                // One owner per account, datasource and connector: two entries differing only in
                // allow_create would leave the creation right ambiguous.
                String owner = writer.getAccountId() + "\u0000" + writer.getDatasourceId() + "\u0000"
                        + (writer.hasConnectorId() ? writer.getConnectorId() : "");
                if (!writers.add(owner)) {
                    throw new IllegalArgumentException("principal '" + binding.getPrincipal()
                            + "' lists intake writer '" + writer.getDatasourceId()
                            + (writer.hasConnectorId() ? "' via connector '" + writer.getConnectorId() : "")
                            + "' in account '" + writer.getAccountId() + "' twice");
                }
            }
        }
        return document;
    }

    /**
     * Loads and verifies a binding file. The document is binary protobuf: the file name
     * must end in {@code .binpb} or {@code .pb}, and anything else is refused naming the
     * accepted forms.
     *
     * @param file the binding file
     * @return the verified document
     * @throws IOException when the file cannot be read
     */
    public static AccountBindings load(Path file) throws IOException {
        String name = file.getFileName().toString();
        if (!name.endsWith(".binpb") && !name.endsWith(".pb")) {
            throw new IllegalArgumentException("account bindings file '" + name
                    + "' must end in .binpb or .pb");
        }
        try {
            return requireWellFormed(AccountBindings.parseFrom(Files.readAllBytes(file)));
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException(
                    "account bindings file '" + name + "' does not parse: " + e.getMessage(), e);
        }
    }

    /**
     * The resolver the environment names: the file at {@value #ENV_ACCOUNT_BINDINGS}, or
     * empty when the variable is unset. A set but blank variable, or a file that does not
     * load, refuses naming the variable.
     *
     * @param environment the process environment
     * @return the configured resolver, or empty when no binding file is named
     */
    public static Optional<AccountBindingResolver> fromEnvironment(Map<String, String> environment) {
        String value = environment.get(ENV_ACCOUNT_BINDINGS);
        if (value == null) {
            return Optional.empty();
        }
        if (value.isBlank()) {
            throw new IllegalArgumentException(ENV_ACCOUNT_BINDINGS
                    + " is set but blank: name a .binpb account bindings file or unset it");
        }
        try {
            return Optional.of(AccountBindingResolver.of(load(Path.of(value))));
        } catch (IOException e) {
            throw new IllegalStateException("failed to read the account bindings named by "
                    + ENV_ACCOUNT_BINDINGS + " at " + value + ": " + e.getMessage(), e);
        }
    }
}
