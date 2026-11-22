package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.v1.IdentityLink;
import ai.protomolt.proto.repo.v1.Principal;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IdentityLinkFileTest {

    private static final Principal UPN = principal("user-principal-name", "alice@example.com");
    private static final Principal ACCOUNT = principal("atlassian-account", "acc-1");
    private static final Principal GROUP = principal("atlassian-group", "g-hr");

    @Test
    void readsDelimitedLinksAndRereadsAChangedFile(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("links.binpb");
        write(file, link(UPN, ACCOUNT));
        var links = IdentityLinkFile.fromEnvironment(Map.of(IdentityLinkFile.ENV_IDENTITY_LINKS, file.toString()))
                .orElseThrow();
        assertThat(links.get().expand(Set.of(UPN))).containsExactly(UPN, ACCOUNT);
        assertThat(links.get()).as("an unchanged file is not reread").isSameAs(links.get());

        write(file, link(UPN, ACCOUNT), link(ACCOUNT, GROUP));
        Files.setLastModifiedTime(file, FileTime.fromMillis(Files.getLastModifiedTime(file).toMillis() + 5_000));
        assertThat(links.get().expand(Set.of(UPN))).containsExactly(UPN, ACCOUNT, GROUP);

        Files.delete(file);
        assertThatThrownBy(links::get).isInstanceOf(UncheckedIOException.class)
                .hasMessageStartingWith("failed to read the identity links named by PROTOMOLT_IDENTITY_LINKS");
    }

    @Test
    void anUnsetVariableMountsNothingAndABadOneRefusesTheBoot(@TempDir Path dir) {
        assertThat(IdentityLinkFile.fromEnvironment(Map.of())).isEmpty();
        assertThatThrownBy(() -> IdentityLinkFile.fromEnvironment(Map.of(IdentityLinkFile.ENV_IDENTITY_LINKS, " ")))
                .hasMessage("PROTOMOLT_IDENTITY_LINKS is set but blank: name an identity link file or unset it");
        assertThatThrownBy(() -> IdentityLinkFile.fromEnvironment(Map.of(IdentityLinkFile.ENV_IDENTITY_LINKS,
                dir.resolve("missing.binpb").toString()))).isInstanceOf(UncheckedIOException.class);
    }

    private static void write(Path file, IdentityLink... links) throws IOException {
        var out = new ByteArrayOutputStream();
        for (IdentityLink link : links) link.writeDelimitedTo(out);
        Files.write(file, out.toByteArray());
    }

    private static Principal principal(String type, String identity) {
        return Principal.newBuilder().setIdentityType(type).setIdentity(identity).build();
    }

    private static IdentityLink link(Principal from, Principal to) {
        return IdentityLink.newBuilder().setFrom(from).setTo(to).build();
    }
}
