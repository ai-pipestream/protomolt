package ai.protomolt.proto.account.service;

import ai.protomolt.proto.authz.AccessPolicies;
import ai.protomolt.proto.authz.AccessPolicyCallers;
import ai.protomolt.proto.authz.AccountBindingPolicies;
import ai.protomolt.proto.authz.AccountBindingResolver;
import ai.protomolt.proto.authz.CallerResolver;
import ai.protomolt.proto.authz.OidcCallerResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * The standalone account host's caller seam, as the environment names it. The operator
 * token at {@value AccountServices#ENV_API_TOKEN} is required: it authenticates the TCP
 * listener's process-authority callers and is the credential the repo channel presents.
 * Credential stores: the access policy file at {@value #ENV_ACCESS_POLICY} and OIDC token
 * introspection at {@value OidcCallerResolver#ENV_INTROSPECTION_URL}, asked in that order,
 * first match wins. Tenancy: the account-binding file at
 * {@value AccountBindingPolicies#ENV_ACCOUNT_BINDINGS}. With no credential store only the
 * operator token authenticates. Account bindings without a credential store bind nothing
 * anyone can present and refuse at boot naming the variables.
 *
 * @param apiToken the nonblank operator token
 * @param resolver the credential stores, or null when only the operator token authenticates
 * @param bindings the account bindings; binds no principal when the variable is unset
 */
record AccountCallerConfiguration(String apiToken, CallerResolver resolver,
        AccountBindingResolver bindings) {

    /** Environment variable naming the access policy file read at boot: {@value}. */
    static final String ENV_ACCESS_POLICY = "PROTOMOLT_ACCESS_POLICY";

    private static final Logger LOG = LoggerFactory.getLogger(AccountCallerConfiguration.class);

    AccountCallerConfiguration {
        if (apiToken == null || apiToken.isBlank()) {
            throw new IllegalArgumentException(AccountServices.ENV_API_TOKEN
                    + " is required: the account host's TCP listener and its repo channel"
                    + " authenticate with the operator token");
        }
        Objects.requireNonNull(bindings, "bindings");
    }

    /** Resolves the seam from {@code environment}; misconfiguration refuses naming the variable. */
    static AccountCallerConfiguration fromEnvironment(Map<String, String> environment) {
        String apiToken = environment.get(AccountServices.ENV_API_TOKEN);
        if (apiToken == null || apiToken.isBlank()) {
            throw new IllegalArgumentException(AccountServices.ENV_API_TOKEN
                    + " is " + (apiToken == null ? "unset" : "blank")
                    + ": the account host's TCP listener and its repo channel authenticate"
                    + " with the operator token");
        }
        List<CallerResolver> stores = new ArrayList<>();
        String policyFile = environment.get(ENV_ACCESS_POLICY);
        if (policyFile != null) {
            if (policyFile.isBlank()) {
                throw new IllegalArgumentException(ENV_ACCESS_POLICY
                        + " is set but blank: name an access policy file or unset it");
            }
            try {
                stores.add(new AccessPolicyCallers(AccessPolicies.load(Path.of(policyFile))));
            } catch (IOException e) {
                throw new IllegalStateException("failed to read the access policy named by "
                        + ENV_ACCESS_POLICY + " at " + policyFile + ": " + e.getMessage(), e);
            }
            LOG.info("access policy mounted from {}", policyFile);
        }
        OidcCallerResolver oidc = OidcCallerResolver.fromEnvironmentMap(environment);
        if (oidc != null) {
            stores.add(oidc);
            LOG.info("OIDC caller store mounted at {}",
                    environment.get(OidcCallerResolver.ENV_INTROSPECTION_URL));
        }
        Optional<AccountBindingResolver> bindings =
                AccountBindingPolicies.fromEnvironment(environment);
        if (bindings.isPresent() && stores.isEmpty()) {
            throw new IllegalArgumentException(AccountBindingPolicies.ENV_ACCOUNT_BINDINGS
                    + " is set but no caller store is: account bindings need "
                    + ENV_ACCESS_POLICY + " or " + OidcCallerResolver.ENV_INTROSPECTION_URL);
        }
        if (bindings.isEmpty() && !stores.isEmpty()) {
            LOG.warn("{} is unset: scoped callers authenticate but are bound to no account,"
                    + " so every account read refuses them",
                    AccountBindingPolicies.ENV_ACCOUNT_BINDINGS);
        }
        CallerResolver resolver = stores.isEmpty() ? null
                : stores.size() == 1 ? stores.getFirst() : CallerResolver.chain(stores);
        return new AccountCallerConfiguration(apiToken, resolver,
                bindings.orElseGet(AccountBindingResolver::none));
    }

    /** Names the parts without the operator token, which never reaches a log line. */
    @Override
    public String toString() {
        return "AccountCallerConfiguration[apiToken=<redacted>, resolver="
                + (resolver == null ? "none" : resolver.getClass().getSimpleName())
                + ", bindings=" + bindings.getClass().getSimpleName() + "]";
    }
}
